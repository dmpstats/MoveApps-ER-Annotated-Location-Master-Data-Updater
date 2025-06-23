library(move2)
library(lubridate)
library(httr2)
library(rlang)
library(dplyr)
library(cli)
library(units)
library(glue)
library(sf)

# NOTES:
# - Cols for cluster ID and behaviour must be stored as they are used to metrics calculation 


# wee helpers
`%!in%` <- Negate(`%in%`)
not_null <- Negate(is.null)


rFunction = function(data, 
                     api_hostname, #api_server-__
                     api_token,
                     cluster_id_col = "clust_id",
                     #behav_col = NULL,
                     lookback = 30L,
                     store_cols_str = NULL) {
  
  # TODO's
  # - Additional cols
  #    * [?] List columns required for the Metrics App and ensure they are stored

  
  # Input Validation ----------------------------------------------------------
  
  logger.info("Checking inputs")
  
  ## Required Column IDs
  check_col_dependencies(
    id_col = cluster_id_col, 
    app_par_name = "Cluster ID Column", 
    dt = data, 
    suggest_msg = paste0(
      "Use clustering Apps such as {.href [Avian Cluster Detection](https://www.moveapps.org/apps/browser/81f41b8f-0403-4e9f-bc48-5a064e1060a2)} ",
      "earlier in the workflow to generate the required column."),
    proceed_msg = paste0("Check on required columns over - all good!")
  )
  
  if(!is.integer(lookback)) cli::cli_abort("{.arg lookback} must be an {.cls integer}.")
  
  
  # Pre-processing ------------------------------------------------------
  logger.info("Performing top-level pre-processing.")
  
  ## utility variables in input data ----
  tm_id_col <- mt_time_column(data)
  trk_id_col <- mt_track_id_column(data)
  dt_crs <- sf::st_crs(data)  # CRS
  sf_col <- attr(data, "sf_column") # geometry column
  
  ## Location coordinates  ------
  ### Add lat/long columns to input data, if absent; otherwise, ensure they're 
  ### named a "lat"/lon 
  if(!any(grepl("^lat", names(test)))){
    lon_lat <- if(sf::st_is_longlat(data)){
      sf::st_coordinates(data)
    } else {
      data |>
        sf::st_transform(4326) |>
        sf::st_coordinates()
    }
    data$lon <- lon_lat[, 'X']
    data$lat <- lon_lat[, 'Y']
  } else{
    data <- data |> 
      dplyr::rename_with(~"lat", .cols = dplyr::matches("^lat")) |> 
      dplyr::rename_with(~"lon", .cols = dplyr::matches("^lon"))
  }
  
  ## Additional columns ---------
  ### Parse name of non-tracking attributes to include in upload
  if(is.null(store_cols_str) || (length(store_cols_str) == 1 && nchar(store_cols_str) == 0)){
    # the default NULL stores all non-tracking columns (i.e. except locations and timne)
    store_cols <- setdiff(names(data), c(tm_id_col, "lat", "lon"))
  }else {
    # parse requested event list field names into a vector (allows comma or semicolon)
    store_cols_parsed <- unlist(strsplit(store_cols_str,",|;"))
    store_cols <- gsub("\\s+", "", store_cols_parsed)
  }
  
  ### check presence in data
  if (!is.null(store_cols) && any(store_cols %!in% names(data))) {
    stop(paste0(
      "Specified additional columns ", paste(store_cols[store_cols %!in% names(data)], collapse = ", "),
      " are not present in the input data. Please ensure these columns exist."
    ))
  }
  
  
  ### Introspect the class/type of traced store_cols
  store_cols_profile <- purrr::map(store_cols, function(col){
    x <- data[[col]]
    data.frame(
      store_col = col,
      cls = class(x)[[1]], # top-level class
      unts = ifelse(inherits(x, "units"), units::deparse_unit(x), NA)
    )
  }) |> 
    purrr::list_rbind()
  
  
  ## Build base URL --------
  if(grepl("http", api_hostname)){
    api_hostname <- httr2::url_parse(api_hostname)$hostname
  }
  
  api_base_url <- paste0("https://", api_hostname, "/api/v1.0/")
  
  
  # Fetch Historical Data --------------------------------------- 
  logger.info("Fetching Historical Tracking Data from ER.")
  
  hist_dt <- fetch_hist(
    api_base_url = api_base_url,
    token = api_token, 
    min_date = min(data[[tm_id_col]]) - lubridate::days(lookback)
  )
  
  
  # Merge Historical and Current data --------------------------------------- 
  
  merged_dt <- merge_magic_fn(data, hist_dt, ...)
  

  
  # Send new observations to ER  ----------------------------------------------
  logger.info("Uploading newly observed track locations to ER.")
  
  merged_dt |> 
    dplyr::filter(observation_status == "new") |> 
    send_new_obs(tm_id_col, trk_id_col, store_cols, api_base_url, token)
  
  
  # Patch updated historical observations in ER  --------------------------------
  logger.info("Updating historical observations with latest available data to ER.")
  
  merged_dt |> 
    dplyr::filter(observation_status == "updated") |> 
    patch_obs(store_cols, api_base_url, token)
  
  
  # Prepare output data ------------------------------------------------------
  # 1. keep only observations that are in active clusters (AND un-clustered?)
  # 2. Coerce as <move2>
  logger.info("Updating historical observations with latest available data to ER.")
  
  ouput <- merged_dt |>
    dplyr::filter(cluster_status %in% c("active", "none")) |> 
    move2::mt_as_move2()
  
  
  logger.info(glue::glue("{symbol$star} Finished update to master location data in ER."))
  output
  
}



# -- Helper Functions ===========================================================

check_col_dependencies <- function(id_col, app_par_name, dt, suggest_msg, proceed_msg,  
                       call = rlang::caller_env(), arg = rlang::caller_arg(id_col)){
  
  dt_cols <- colnames(dt)
  
  if(is.null(id_col)){
    logger.fatal(paste0("`", arg, "` is missing."))
    cli::cli_abort(c(
      "Parameter '{app_par_name}' ({.arg {arg}}) must be a string, not `NULL`.",
      "i" = "Please provide a valid column name for this parameter."
    ), call = call)
    
  } else if(!is.character(id_col) || length(id_col) > 1){
    logger.fatal(paste0("`", arg, "` must be a string."))
    cli::cli_abort(
      "Parameter '{app_par_name}' ({.arg {arg}}) must be a string.",
      call = call)
    
  } else if (id_col %!in% dt_cols) {++
    logger.fatal(paste0(
      "Input data does not have column '", id_col,"'. Please provide a ",
      "valid column name with cluster ID annotations."
    ))
    cli::cli_abort(c(
      "Specified column name {.arg {id_col}} must be present in the input data.",
      "!" = "Please provide a valid column name for parameter '{app_par_name}' ({.arg {arg}}).",
      "i" = suggest_msg
    ), call = call)
  } 
  

  if(all(is.na(dt[[id_col]]))){
    logger.fatal(glu:e:glue("Column {.code {id_col}} in input data contains universally NAs"))
    cli::cli_abort(c(
      "Column {.code {id_col}} in input dataset contains only NAs. Unable to proceed with cluster updating.",
      "i" = paste0(
        "Input data must include annotations for at least one cluster"
      )
    ),
    class = "no-clusters-in-input"
    )
  }
  
  
  logger.info(proceed_msg)
  
}




# ////////////////////////////////////////////////////////////////////////////////////
#' Download a subset of historic observations from master dataset stored in ER
#' 
#' @param api_base_url `<character>`, the base URL of the API endpoint used to fetch
#'   historical data (e.g., `"https://api.example.org/v1"`).
#' @param token `<character>`, a valid authentication token used to authorize the
#'   request.
#' @param min_date `<POSIXt/POSIXct/POSIXlt>`, the start date-time for filtering
#'   observations (inclusive). If `NULL` (default, no lower date-time bound is
#'   applied.
#' @param max_date `<POSIXt/POSIXct/POSIXlt>`, the end date-time for filtering
#'   observations (inclusive). If `NULL` (default), no upper date-time bound is
#'   applied.
#' @param include_details logical, If `TRUE` (default), includes additional
#'   attributes in the response payload. If `FALSE`, returns only tracking
#'   fields, i.e. location and time
#' @param page_size Integer. Number of records to retrieve per page. Defaults to
#'   5000. Larger values may reduce request frequency but can increase memory
#'   usage.
#' 
#' @details 
#' - process split into 2 fetching requests:
#'      1. ALL observations tagged with clusters that are currently active 
#'      (`filter` == 8)
#'      2. For a given time-window: observations tagged with closed clusters AND
#'      un-clustered observations
#' - Output: attributes are renamed to ensure compatibility with original 
#'   `<move2>` data
#' 
fetch_hist <- function(api_base_url, 
                       token, 
                       min_date = NULL, 
                       max_date = NULL, 
                       include_details = TRUE,
                       page_size = 5000#, 
                       #cluster_status_filter = c("active", "all-status")
                       ){
  
  # Input validation and pre-processing -----------------------------
  
  # ## cluster-status  filtering
  # cluster_status_filter <- rlang::arg_match(cluster_status_filter)
  # 
  # exc_flag <- switch(cluster_status_filter,
  #   active = 8,
  #   #closed = 4,
  #   'all-status' = "null"
  # )
  
  # Retrieve observations ---------------------------------------
  
  # ALL observations in active clusters up to max_date
  obs_cluster_actv <- get_obs(
    api_base_url = api_base_url, 
    token = token, 
    filter = 8,
    min_date = NULL, 
    max_date = max_date, 
    created_after = NULL,
    include_details = include_details,
    page_size = page_size
  )
  
  
  # unclustered and closed observations from min_date to max_date
  obs_uncluster <- get_obs(
    api_base_url = api_base_url, 
    token = token, 
    filter = 0,
    min_date = min_date, 
    max_date = max_date, 
    created_after = NULL,
    include_details = include_details,
    page_size = page_size
  ) |> 
    dplyr::filter(
      cluster_status != "closed"
    )
  
  # stack-up the two datasets
  obs <- dplyr::bind_rows(obs_cluster_actv, obs_uncluster)
  
  
  # Retrieve required complementary data ----------------------------------
  # Get manufacturer_id/tag_id and subject_name/individual_local_identifier 
  # for each sources
  sources <- unique(obs$source)
  
  sources_aux <- sources |> 
    purrr::map(function(s){
      manufacturer_id <- get_source_details(s, api_base_url, token)[["manufacturer_id"]]
      
      subject_name <- get_source_subjects(s, api_base_url, token) |> 
        purrr::map_chr( \(sbj) sbj[["name"]])
      
      if(length(subject_name) > 1){
        cli::cli_abort(c(
          "Each `tag_id`/`manufacturer_id` must be uniquely associated with a single subject.",
          x = "Detected a one-to-many relationship between `manufacturer_id` and `subject_name`, which is not currently allowed."
        ))
      }
      
      dplyr::tibble(
        manufacturer_id = ifelse(length(manufacturer_id) == 0, NA, manufacturer_id), 
        subject_name = ifelse(length(subject_name) == 0, NA, subject_name)
      )
      
    }) |> 
    setNames(sources) |> 
    purrr::list_rbind(names_to = "source")
  

  # Prepare and output ----------------------------------
  # Rename columns for compatibility with input data from workflow
  dplyr::right_join(sources_aux, obs, by = "source") |> 
    dplyr::select(-exclusion_flags) |> 
    dplyr::mutate(
      obs_id = id,
      tag_id = manufacturer_id,
      individual_local_identifier = subject_name,
      timestamp = lubridate::as_datetime(recorded_at, tz = "UTC"), 
      .before = 1,
      .keep = "unused"
    )
}




# /////////////////////////////////////////////////////////////////////////////////
#' Send new observations to EarthRanger
#' 
#' @param new_dt a data.frame, containing new observations to be pushed to
#'   EarthRanger.
#' @param tm_id_col character, name of the column in `new_dt` that contains the
#'   timestamps of each observation.
#' @param trk_id_col character, the name of the column in `new_dt` that
#'   identifies the associated track ID for each observation.
#' @param store_cols character vector, Names of additional columns in `new_dt`
#'   to be stored alongside the tracking data in EarthRanger.
#' @param api_base_url `<character>`, the base URL of the API endpoint used to fetch
#'   historical data (e.g., `"https://api.example.org/v1"`).
#' @param token `<character>`, a valid authentication token used to authorize the
#'   request.
#' 
#' @details
#' - Push new observations via a Radio Agent POST Request, so that new tag/subjects
#'   are automatically added on the ER side.
#' - NOTE: This approach is a bit hacky, as RA requests don't allow to pass 
#'   exclusions flags. So taking the workaround: RA-POST(new) -> GET(just posted) -> 
#'   -> PATCH(exclusion_flags on just posted)

send_new_obs <- function(new_dt, tm_id_col, trk_id_col, store_cols, api_base_url, token){
  
  # 1. Post all attributes except cluster_status ----- 
  # 
  # currently is not possible to include  exclusion flag in RA requests. ER
  # (Chris) promised this functionality could be available soon
  
  posting_dttm <- lubridate::now() - lubridate::seconds(60)
  ra_post_obs(
    data = new_dt, 
    tm_id_col = tm_id_col, 
    trk_id_col = trk_id_col, 
    store_cols = store_cols, 
    api_base_url = api_base_url, 
    token = token, 
    provider_key = "moveapps_ann_locs", 
    batch_size = 100
  )
  
  # 2. Retrieve just sent observations  ---------------------------
  er_obs <- get_obs(
    api_base_url = api_base_url, 
    token = token, 
    created_after = posting_dttm, 
    include_details = FALSE,
  ) |> 
    dplyr::mutate(
      timestamp = lubridate::ymd_hms(recorded_at, tz = "UTC"),
      .keep = "unused"
    )
  
  # 3. Re-merge  -----------------------
  # Link cluster_status (aka exclusion_flag) to each observation IDs
  obs_excl_flags <- dplyr::right_join(new_dt, er_obs, by = c("timestamp", "lat", "lon")) |> 
    dplyr::mutate(
      exclusion_flags = ifelse(cluster_status == "active", 8, 0),
      er_obs_id = id,
      .keep = "unused"
    ) |> 
    select(er_obs_id, exclusion_flags)
    
  
  # 4. Patch observations with latest exclusion_flag -------------------------
  patch_obs(obs_excl_flags, api_base_url = api_base_url, token = token)

}




# ///////////////////////////////////////////////////////////////////////////////
#' Low-level helper to perform Radio-Agent POST Observations request
#' 
#' @param data a data.frame, data to be pushed to EarthRanger.
#' @param tm_id_col character, name of the column in `data` that contains the
#'   timestamps of each observation.
#' @param trk_id_col character, the name of the column in `data` that
#'   identifies the associated track ID for each observation.
#' @param store_cols character vector, Names of additional columns in `data`
#'   to be stored alongside the tracking data in EarthRanger.
#' @param api_base_url `<character>`, the base URL of the API endpoint used to fetch
#'   historical data (e.g., `"https://api.example.org/v1"`).
#' @param token `<character>`, a valid authentication token used to authorize the
#'   request.
#' @param provider_key character string, specifying the natural key for the
#'   source provider, as defined in EarthRanger. Expects such key is already
#'   defined in ER's Observation's system. 
#' @param batch_size numeric, the number of observations to include in each
#'   batch POST request. ER's documentation suggest sending as many as 100
#'   observations per request.
#' 
#' @details
#' - RA-POST requests allow for automatic creation of new manufactor's/source ID 
#' and subject's names
#' - Requirements: "tag_id" and "individual_local_identifier" MUST be in data
ra_post_obs <- function(data, 
                        tm_id_col, 
                        trk_id_col, 
                        store_cols, 
                        api_base_url, 
                        token, 
                        provider_key = "moveapps_ann_locs",
                        batch_size = 200){
  
  # Pre-Processing --------------------------------------------------------
  # append API's endpoint for request
  api_endpnt <- file.path(api_base_url, "sensors/dasradioagent", provider_key, "status")
  
  # bind "track_id" column to store as additional data
  store_cols <- c(store_cols, "track_id")
  
  # Manipulate data for ER's RA POSTing -----------------------------------
  #   - add/rename required cols
  #   - format cols for json serialization
  #   - nest data for multiple observation POSTing
  #   - generate batch IDs for batch posting
  dt_jsonable <- data |> 
    as.data.frame() |> 
    dplyr::mutate(
      # date-time cols formatted as ISO 8601 strings
      dplyr::across(dplyr::where(~inherits(.x, "POSIXt")), \(x) format(x, "%Y-%m-%d %X%z")),
      # convert any columns of class integer64 to string
      dplyr::across(dplyr::where(~inherits(.x, "integer64")), as.character),
      #drop units
      dplyr::across(dplyr::where(~inherits(.x, "units")), .fns = \(x) units::set_units(x, NULL))
    ) |> 
    dplyr::mutate(
      recorded_at = .data[[tm_id_col]],
      manufacturer_id = as.character(tag_id),
      subject_name = as.character(individual_local_identifier),
      subject_type = "Wildlife",
      subject_subtype = "Unassigned", 
      source_type = "tracking_device",
      track_id = .data[[trk_id_col]],
      .keep = "unused"
    )  |> 
    tidyr::nest(
      location = c(lat, lon),
      additional = dplyr::any_of(store_cols),
      .by = c(recorded_at:subject_subtype)
    ) |> 
    dplyr::mutate(
      location = lapply(location, as.list),
      additional = lapply(additional, as.list),
      batch_id = 1:n() %/% batch_size
    )
  
  # Perform, batch RA Post Request ------------------------------
  logger.info("POSTing observations to ER...")
  
  results <- dt_jsonable |> 
    split(~batch_id) |> 
    purrr::map_int(function(batch) {
      
      # Prepare request
      req <- httr2::request(api_endpnt) |>
        httr2::req_auth_bearer_token(token) |> 
        httr2::req_headers(
          "Accept" = "application/json",
          "Content-Type" = "application/json"
        ) |> 
        httr2::req_body_json(batch)
      
      #req |> req_dry_run()
      
      # Send request 
      req |> 
        httr2::req_error(body = \(resp) httr2::resp_body_string(resp)) |> 
        httr2::req_perform() |> 
        resp_status()

    }, 
    .progress = list(format = "Uploading batch #{pb_current}/{pb_total} [{pb_rate}]")# | {cli::pb_eta_str}")
    )
  
  # Log Results ------------------------------------------------------
  successful_batches <- sum(results %in% c(200, 201))
  total_batches <- length(results)
  logger.info(glue::glue("{symbol$tick} {successful_batches} of {total_batches} batches of observations posted successfully to EartRanger."))
  
}



# //////////////////////////////////////////////////////////////////////////////
#' Perform observations GET requests to EarthRanger
#' 
#' Same parameters as `fetch_hist()`, above
get_obs <- function(api_base_url, 
                    token, 
                    filter = NULL, 
                    min_date = NULL, 
                    max_date = NULL, 
                    created_after = NULL, 
                    include_details = TRUE, 
                    page_size = 1000){
  
  # Input Validation & processing ----------------------------------------
  #
  ## Note dttms need to be formatted as ISO 8601 strings (required by the API)
  
  ## `filter`
  if(not_null(filter)){
    if(!is.numeric(filter)){
      cli::cli_abort("{.arg filter} must be numeric")
    }
  } else{
    # "null" filter means all observations regardless associated exclusion flags
    filter <-  "null"
  }
  
  ## `min_date`
  if(not_null(min_date)){
    if(!inherits(min_date, "POSIXt")) {
      cli::cli_abort("{.arg min_date} must be {.cls POSIXt}")
    }
  }else{
    ## If min_date is not provided, use 31 days from sys.time
    min_date <- Sys.time() - lubridate::days(31)
  }
  
  min_date <- format(min_date, "%Y-%m-%d %X%z")
    
  ## `max_date`
  if(not_null(max_date)){
    if(!inherits(max_date, "POSIXt")) {
      cli::cli_abort("{.arg max_date} must be {.cls POSIXt}")
    }
    max_date <- format(max_date, "%Y-%m-%d %X%z")
  }
  
  ## `created_after`
  if(not_null(created_after)){
    if(!inherits(created_after, "POSIXt")) {
      cli::cli_abort("{.arg created_after} must be {.cls POSIXt}")
    }
    created_after <- format(created_after, "%Y-%m-%d %X%z")
  }


  # GET observations request  ---------------------------------------
  api_endpnt <- file.path(api_base_url, "observations")
  
  ## Initialize variables for pagination
  all_results <- list()
  next_page_url <- api_endpnt
  
  cli::cli_progress_bar("Retrieving historic observations from ER - page")
  
  ## Loop to retrieve all pages
  while (!is.null(next_page_url)) {
    
    # Prepare the request with additional query parameters
    req <- httr2::request(next_page_url) |>
      httr2::req_url_query(
        filter = filter,
        since = min_date,
        until = max_date,
        created_after = created_after,
        page_size = page_size,
        include_details = tolower(as.character(include_details)) # Convert logical to string
      ) |>
      httr2::req_auth_bearer_token(token) |> 
      httr2::req_headers("accept" = "application/json")
    
    # Perform the request
    res <- req |> 
      httr2::req_error(body =  \(resp){
        "Failed to request observations historical data."
      }) |> 
      httr2::req_perform()
    
    # Parse the response -----------
    data <- httr2::resp_body_json(res, simplifyVector = TRUE) |> 
      purrr::pluck("data") 

    # isolate dataset with observations
    obs <- data[["results"]]
    
    # If location is non-null, unnest the column
    if (not_null(obs$location)) {
      obs <- obs |>
        tidyr::unnest(location, names_sep = "_") |> 
        dplyr::rename(
          lat = "location_latitude",
          lon = "location_longitude"
        )
    }
    
    # observation_details contains a nested list equal in 
    # length to the data. We want to first turn this into a nested
    # column, and then unnest:
    if (!is.null(obs$observation_details)) {
      
      if (class(obs$observation_details) == "data.frame") {
        observation_details_out <- obs$observation_details
      } else {
        observation_details_out <- obs$observation_details |>
          lapply(data.frame) |> 
          # If any entry is a 0-row data frame or empty list, replace it 
          # with a data.frame containing only PLACEHOLDER = NA
          purrr::map( \(x){ if (nrow(x) == 0 || is.null(x)) data.frame(PLACEHOLDER = NA) else x}) |> 
          dplyr::bind_rows() |> 
          dplyr::select(-dplyr::any_of("PLACEHOLDER")) # this was just for merging
      }
      
      # Validate that nrow(observation_details_out) matches nrow(data)
      if (nrow(observation_details_out) != nrow(obs)) {
        cli::cli_abort(
          "Observation details length mismatch: {nrow(observation_details_out)} vs {nrow(obs)}"
        )
      }
      
      # Bind observation_details_df back to the main data frame
      obs <- obs |>
        # Drop the main observation_details col
        dplyr::select(-"observation_details") |>
        dplyr::bind_cols(observation_details_out)
    }

    # Append the current page of results to the list
    all_results[[next_page_url]] <- obs
    
    # Update the next page URL for pagination
    next_page_url <- data[["next"]]
    
    cli::cli_progress_update()
  }
  
  cli::cli_progress_done()
  
  # discard empty lest elements 
  all_results <- purrr::compact(all_results)
  
  # Combine all results into a data frame
  combined_obs <- if (length(all_results) > 0) {
    do.call(bind_rows, lapply(all_results, as.data.frame))
  } else {
    logger.warn("GET request for historic observations succeeded but returned an empty dataset.")
    data.frame()
  }
  
  combined_obs
}



# ///////////////////////////////////////////////////////////////////////////////
#' Helper to fetch information about the transmitting device (i.e. manufactors ID)
#' 
#' @param src character, the UUID of the source
get_source_details <- function(src, api_base_url, token){
  
  req_url <- file.path(api_base_url, "source", src)
  
  req_src <- httr2::request(req_url) |>
    httr2::req_auth_bearer_token(token) |> 
    httr2::req_headers("accept" = "application/json")
  #req_src |> req_dry_run()
  
  req_src |> 
    #httr2::req_error(~FALSE)|> 
    httr2::req_perform() |> 
    resp_body_json() |> 
    purrr::pluck("data")
}



# ///////////////////////////////////////////////////////////////////////////////
#' Helper to fetch information about the tagged subject
#' 
#' @param src character, the UUID of the source
get_source_subjects <- function(src, api_base_url, token){
  
  req_url <- file.path(api_base_url, "source", src, "subjects")
  
  req_src <- httr2::request(req_url) |>
    httr2::req_auth_bearer_token(token) |> 
    httr2::req_headers("accept" = "application/json")
  #req_src |> req_dry_run()
  
  req_src |> 
    #httr2::req_error(~FALSE)|> 
    httr2::req_perform() |> 
    resp_body_json() |> 
    purrr::pluck("data")
}



# ///////////////////////////////////////////////////////////////////////////////
#' Perform observations PATCH request to EartRanger
#' 
#' @param store_cols names of columns to store in Master Observations data in
#'   ER. If `NULL` (default), the existing attributes in the "additional" field
#'   are preserved. If provided, the field will be fully overwritten with the
#'   specified columns and their values — **use with caution**.
#'   
#'  @details
#' General assumptions and scope:
#'  - location columns must be named "lat"/"lon"
#'  - Currently this Patch function is (deliberately) limited to only update 
#'  the following Obs fields: "location", "additional" and "exclusion_flags"
#'   
patch_obs <- function(data,
                      store_cols = NULL,
                      api_base_url, 
                      token){
  
  # Input Validation -----------------------------------------------------------
  if ("er_obs_id" %!in% names(data)) {
    cli::cli_abort("{.arg data} must contain column {.val er_obs_id} to match observations to existing records.")
  }
  
  # Perform PATCH requests (1 per observation) ----------------------------
  logger.info("PATCHing requests for observation updates...")
  
  results <- data |> 
    dplyr::group_split(er_obs_id) |> 
    purrr::map_int(
      function(obs){
        #browser()
        api_endpnt <- file.path(api_base_url, "observation", obs$er_obs_id)
        
        body_list <- list(
          locations = list(lat = obs$lat, lon = obs$lon),
          exclusion_flags = obs$exclusion_flags,
          additional = as.list(obs[store_cols])
        ) |> 
          # essentially, drop additional field if none is passed
          purrr::compact()
        
        req <- httr2::request(api_endpnt) |> 
          httr2::req_auth_bearer_token(token) |> 
          httr2::req_method("PATCH") |> 
          httr2::req_headers(
            "Accept" = "application/json",
            "Content-Type" = "application/json"
          ) |> 
          httr2::req_body_json(body_list)
        
        req |> 
          httr2::req_error(body = \(resp) httr2::resp_body_string(resp)) |> 
          httr2::req_perform() |> 
          httr2::resp_status()
        
      }, 
      .progress = list(format = "Updating observation #{pb_current}/{pb_total} [{pb_rate}] | {cli::pb_eta_str}")
    )
  
  ## Log results ------------------------------------------------------
  successful_requests <- sum(results %in% c(200, 201))
  total_requests <- length(results)
  logger.info(glue::glue("{symbol$tick} {successful_requests} of {total_requests} observations updated successfully."))

}








# //////////////////////////////////////////////////////////////////////////////////
# er_check_status <- function(api_base_url, token){
#   
#   status_endpoint <- paste0(api_base_url, "/status/")
#   
#   req <- httr2::request(status_endpoint) |>
#     httr2::req_headers(
#       "accept" = "application/json"#,
#       #"Authorization" = paste0('Bearer ', token)
#     )
#   
#   res <- req |> 
#     #req_error(is_error = ~ FALSE) |> 
#     req_perform()
#   
#   #httr2::resp_body_json(res)$status$message
#   
#   invisible()
#   
#   # if(httr2::resp_status(res) == 404){
#   #   logger.fatal(cli::cli_text("No API found under Hostname {api_hostname}."))
#   #   cli::cli_abort("No API found under Hostname {api_hostname}.")
#   # }
#   
# } 



merge_magic_fn <- function(data, hist){
  
  logger.info("Merrrrrrging data...")
  
  move2::mt_stack(data, hist)
  
}




