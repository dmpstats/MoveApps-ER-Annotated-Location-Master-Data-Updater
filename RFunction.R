library(move2)
library(lubridate)
library(httr2)
library(rlang)
library(dplyr)
library(cli)
library(units)
library(glue)
library(sf)
library(purrr)
library(tidyr)
library(Gmedian)
library(bit64)
library(ids)
# library(cowplot)
# library(ggplot2)

# wee helpers
`%!in%` <- Negate(`%in%`)
not_null <- Negate(is.null)


# Global objects

## Key attributes to trace during the API and data merging logics 
cluster_cols <- c("cluster_status", "cluster_uuid")
mv2_track_cols <- c("tag_id", "individual_local_identifier", "deployment_id", "individual_id", "track_id", "study_id")

## Exclusion flag value to tag obs in ACTIVE clusters
active_flag <- bit64::as.integer64(1311673391471656960)


#' OVERALL NOTES
#' 
#' Correspondence between ER == Move2 Attributes:
#'    - "manufacturer_id" (defines a "source") == "tag_id" 
#'    - "subject_name" == "individual_local_identifier"
#'    - "recorded_at" == mt_time_column(data)

rFunction = function(data, 
                     api_hostname = NULL,
                     api_token = NULL,
                     cluster_id_col = "clust_id",
                     lookback = 30L,
                     store_cols_str = NULL,
                     days_thresh = 7,
                     dist_thresh = 100,
                     match_criteria = "gmedian",
                     active_days_thresh = 14
                     ) {
  
  # TODO's
  # - generalize function to allow simple updating and storage of data in ER 
  #   (i.e. without cluster tracking and merging)?
  
  # Input Validation ----------------------------------------------------------
  
  logger.info("Checking inputs")
  
  ## `api_hostname` and `api_token`
  if(is.null(api_hostname)){
    cli::cli_abort(c(
      "Argument {.arg api_hostname} is missing.",
      "{.arg api_hostname} must be an {.cls string}, not `NULL`."
    ))
  }
  
  if(is.null(api_token)){
    cli::cli_abort(c(
      "Argument {.arg api_token} is missing.",
      "{.arg api_token} must be an {.cls string}, not `NULL`."
    ))
  }
  
  ## `lookback`
  if(!is.integer(lookback)) cli::cli_abort("{.arg lookback} must be an {.cls integer}.")

  ## `cluster_id_col`
  check_col_dependencies(
    id_col = cluster_id_col, 
    app_par_name = "Cluster ID Column", 
    dt = data,
    suggest_msg = paste0(
      "Use clustering Apps such as {.href [Avian Cluster Detection](https://www.moveapps.org/apps/browser/81f41b8f-0403-4e9f-bc48-5a064e1060a2)} ",
      "earlier in the workflow to generate the required column."),
    proceed_msg = paste0("Finished check on required columns in input data - all good!")
  )
 
  
  
  # Pre-processing ------------------------------------------------------
  logger.info("Performing top-level pre-processing.")
  
  ## Utility variables in input data ----
  tm_id_col <- mt_time_column(data)
  trk_id_col <- mt_track_id_column(data)
  dt_crs <- sf::st_crs(data)  # CRS
  sf_col <- attr(data, "sf_column") # geometry column
  
  ## Location coordinates  ------
  ### Add lat/long columns to input data, if absent; otherwise, ensure they're 
  ### named a "lat"/lon 
  if(!any(grepl("^lat", names(data)))){
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
    # ugly way to rename lat/lon cols, as move2 objects loose track attributes
    # when passed to dplyr::rename functions
    names(data)[grep("^lat", names(data))] <- "lat"
    names(data)[grep("^lon", names(data))] <- "lon"
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
  
  ### ensure listed store_cols are in data
  if (!is.null(store_cols)){
    missing_store_cols <- store_cols %!in% names(data)
    if(sum(missing_store_cols) > 0){
      cli::cli_abort(c(
        "Attribute{?s} {.val {store_cols[missing_store_cols]}} not found in the input data.",
        "i" = "Ensure all names listed in {.arg store_cols} match column names in the input dataset."
      ))
    }
  }
  
  
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
    unclust_min_date = min(data[[tm_id_col]]) - lubridate::days(lookback), 
    page_size = 500
  )
  
  
  # Merge Historical and Current data --------------------------------------- 
  logger.info("Merge input data with historical data.")
  
  ## Perform clusters matching ------
  
  if(not_null(hist_dt)){
    
    ### Prepare historical data for merging
    hist_dt <- hist_dt |> 
      sf::st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)
    
    ### match clusters
    matched_dt <- match_sf_clusters(
      hist_dt = hist_dt,
      new_dt = data,
      cluster_id_col = cluster_id_col,
      timestamp_col = tm_id_col,
      days_thresh = days_thresh,
      dist_thresh = units::set_units(dist_thresh, "m"),
      match_criteria = match_criteria
    )
    
  } else {
    ## hist_dt is NULL under some controlled situations. Check `fetch_hist()`
    ## documentation. When that's the case we still need to generate an empty
    ## object with a similar structure as the object returned by
    ## `match_sf_clusters()` to perform the tasks below
    matched_dt <- list(
      matched_master_data = dplyr::tibble(
        cluster_uuid = NA_character_, subject_name = NA_character_, manufacturer_id = NA_character_, 
        recorded_at = NA_POSIXct_, er_obs_id = NA_character_, er_source_id = NA_character_, 
        {{cluster_id_col}} := NA_character_, lon = NA_real_, lat = NA_real_
      ) |> 
        sf::st_as_sf(geometry = sf::st_sfc(sf::st_point(c(NA_real_, NA_real_)), crs = sf::st_crs(data))),
      match_table = NULL,
      match_plot = NULL
    )
  }
  
  
  ## Conciliate & merge datasets -----------
  merged_dt <- merge_and_update(
    matched_hist_dt = matched_dt$matched_master_data, 
    new_dt = data, 
    cluster_id_col = cluster_id_col, 
    timestamp_col = tm_id_col, 
    store_cols = store_cols,
    active_days_thresh = active_days_thresh
  ) |> 
    # dropping row created above to deal with NULL `hist_dt`, identified as empty lat/lon
    tidyr::drop_na(lat, lon) 
    
  
  # Send new observations to ER  ----------------------------------------------
  logger.info("Uploading newly observed locations to ER.")
  
  merged_dt |> 
    dplyr::filter(request_type == "POST") |> 
    ra_post_obs(
      tm_id_col = tm_id_col, 
      additional_cols =  c(store_cols, cluster_cols, mv2_track_cols), 
      api_base_url = api_base_url, 
      token = api_token, 
      provider_key = "moveapps_ann_locs", 
      batch_size = 200
    )
  
  
  # Patch updated historical observations in ER  --------------------------------
  logger.info("Updating historical observations with latest available data to ER.")
  
  merged_dt |> 
    dplyr::filter(request_type == "PATCH") |> 
    patch_obs(
      additional_cols = c(store_cols, cluster_cols, mv2_track_cols),
      api_base_url = api_base_url, 
      token = api_token
    )
  
  
  # Prepare output data ------------------------------------------------------
  # 1. Spatial terms: project into same CRS as input data & set same name for geom column
  # 2. Coerce as <move2>, with pre-specified and traced track columns
  logger.info("Preparing data for output")
  
  output <- merged_dt |>
    sf::st_transform(crs = sf::st_crs(data)) |> 
    # safeguard against missing values in track_id
    tidyr::replace_na(list(track_id =  "UNKNOWN")) |> 
    # safeguard against missing values in track-level attributes within each
    # track. Should be removable once the ER-MA data exchanges are streamlined
    dplyr::mutate(
     dplyr::across(any_of(mv2_track_cols), .fns = ~dplyr::first(.x, na_rm = TRUE)),
     .by = track_id
    ) |>
    #dplyr::filter(cluster_status %in% c("ACTIVE", "CLOSE")) |> 
    dplyr::select(dplyr::any_of(c(tm_id_col, store_cols, cluster_cols, mv2_track_cols))) |> 
    move2::mt_as_move2(
      time_column = tm_id_col, 
      track_id_column = "track_id",
      track_attributes = any_of(mv2_track_cols),
      sf_column_name = sf_col
    )
 
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
    
  } else if (id_col %!in% dt_cols) {
    logger.fatal(paste0(
      "Input data does not have column '", id_col, "'. Please provide a ",
      "valid column name with cluster ID annotations."
    ))
    cli::cli_abort(c(
      "Specified column name {.arg {id_col}} must be present in the input data.",
      "!" = "Please provide a valid column name for parameter '{app_par_name}' ({.arg {arg}}).",
      i = suggest_msg
    ), call = call)
  } 
  

  if(all(is.na(dt[[id_col]]))){
    #logger.fatal(glue::glue("Column {.code {id_col}} in input data contains universally NAs"))
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
#' @param unclust_min_date `<POSIXt/POSIXct/POSIXlt>`, the start date-time for
#'   filtering un-clustered observations (inclusive). If `NULL` (default), no
#'   lower date-time bound is applied.
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
#'      1. **ALL** observations tagged with clusters that are currently ACTIVE 
#'      (`filter` == 1311673391471656960)
#'      2. For a given time-window: observations tagged with closed clusters AND
#'      un-clustered observations
#'      
#' @returns 
#' Either a data frame, or a NULL object under the following conditions in ER's Observation Table:
#'  - No records (i.e. 0 observations)
#'  - Absence of attribute "cluster_uuid" in retrieved data
#'  - All observations are in CLOSED clusters
#'  
#' Note: some ER-origin attributes are renamed to ensure compatibility with original 
#'   `<move2>` data
#' 
fetch_hist <- function(api_base_url, 
                       token, 
                       unclust_min_date = NULL, 
                       max_date = NULL, 
                       include_details = TRUE,
                       page_size = 5000#, 
                       #cluster_status_filter = c("active", "all-status")
){
  
  
  # Retrieve observations ---------------------------------------
  
  # ALL observations in active clusters up to max_date
  obs_cluster_actv <- get_obs(
    api_base_url = api_base_url, 
    token = token, 
    filter = active_flag,
    min_date = NULL, 
    max_date = max_date, 
    created_after = NULL,
    include_details = include_details,
    page_size = page_size
  )
  
  
  # get un-clustered and closed observations (tagged as non_excluded) from min_date to max_date
  obs_cluster_non_excluded <- get_obs(
    api_base_url = api_base_url, 
    token = token, 
    filter = 0,
    min_date = unclust_min_date, 
    max_date = max_date, 
    created_after = NULL,
    include_details = include_details,
    page_size = page_size
  )
  
  
  # Handle retrieved datasets -------------------------------------
  # If some o the conditions are met, forces function to return NULL
  
  ## Case 1: both datasets are empty. Two possibilities in ER's Observations table:
  ##   - No records available
  ##   - All observations are tagged with exclusion_flags AND there is none of the obs are in ACTIVE clusters
  if(nrow(obs_cluster_actv) == 0 && nrow(obs_cluster_non_excluded) == 0){
    return(NULL)
  }
  
  ## Case 2: No ACTIVE clusters, but non-excluded data present
  if(nrow(obs_cluster_actv) == 0){
    # "cluster_uuid" attribute not present (e.g. inaugural run)
    if("cluster_uuid" %!in% names(obs_cluster_non_excluded)){
      return(NULL)
    }else{
      # keep only un-clustered observations (i.e. with no cluster UUID annotated)
      obs_cluster_non_excluded <- dplyr::filter(obs_cluster_non_excluded, is.na(cluster_uuid))
    }
    
    # All observations are in CLOSED clusters
    if(nrow(obs_cluster_non_excluded) == 0){
      return(NULL)
    }
  }
  
  # stack-up the two datasets
  obs <- dplyr::bind_rows(obs_cluster_actv, obs_cluster_non_excluded)
  
  
  # Retrieve required complementary data ----------------------------------
  # Get manufacturer_id/tag_id and subject_name/individual_local_identifier 
  # for each source
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
  dplyr::right_join(sources_aux, obs, by = "source") |> 
    dplyr::select(!c(exclusion_flags)) |> 
    dplyr::mutate(
      er_obs_id = id,
      er_source_id = source,
      #tag_id = manufacturer_id,
      #individual_local_identifier = subject_name,
      recorded_at = lubridate::as_datetime(recorded_at, tz = "UTC"), 
      .before = 1,
      .keep = "unused"
    )
}



# ///////////////////////////////////////////////////////////////////////////////
#' Low-level helper to perform Radio-Agent POST Observations request
#' 
#' @param data a data.frame, data to be pushed to EarthRanger.
#' @param tm_id_col character, name of the column in `data` that contains the
#'   timestamps of each observation.
#' @param additional_cols character vector, Names of additional columns in `data`
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
ra_post_obs <- function(data, 
                        tm_id_col, 
                        additional_cols, 
                        api_base_url, 
                        token, 
                        provider_key = "moveapps_ann_locs",
                        batch_size = 200){

  if(nrow(data) == 0){
    logger.warn("  |- No observations to POST - skipping POSTing step.")
    return(NULL)
  }
  
  # input validation -------------------------------------------------------------------
  req_cols <- c("cluster_status", "tag_id", "individual_local_identifier", "lat", "lon")
  miss_cols <- req_cols[req_cols %!in% names(data)]
  if (length(miss_cols) > 0) {
    cli::cli_abort("{.arg data} is missing the following required columns: {.val {miss_cols}}.")
  }
    
  # Pre-Processing --------------------------------------------------------
  # append API's endpoint for request
  api_endpnt <- file.path(api_base_url, "sensors/dasradioagent", provider_key, "status")

  # Manipulate data for ER's RA POSTing -----------------------------------
  #   - add/rename required cols
  #   - format cols for json serialization
  #   - nest data for multiple observation POSTing
  #   - generate batch IDs for batch posting
  #   - flag obs in ACTIVE clusters with appropriate exclusion flags
  dt_jsonable <- data |> 
    data.frame() |>
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
      #source_type = "tracking_device",
      .keep = "unused"
    )  |> 
    dplyr::mutate(
      exclusion_flags = dplyr::if_else(cluster_status == "ACTIVE", as.integer64(active_flag), as.integer64(0), missing = as.integer64(0))
    ) |> 
    tidyr::nest(
      location = c(lat, lon),
      additional = dplyr::any_of(additional_cols),
      .by = c(recorded_at:exclusion_flags)
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
      #browser()
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
#' @param created_after `<POSIXt/POSIXct/POSIXlt>`
#' @param filter integer, used to filter the retrieval of observations based on
#'   a exclusion flag value
#' @param include_details logical, whether to retrieve data stored as
#'   "Additional" data in ER
#' @param page_size Integer. Number of records to retrieve per page. Defaults to
#'   5000. Larger values may reduce request frequency but can increase memory
#'   usage.
#'   
get_obs <- function(api_base_url, 
                    token, 
                    min_date = NULL, 
                    max_date = NULL, 
                    created_after = NULL, 
                    filter = NULL, 
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
    min_date <- format(min_date, "%Y-%m-%d %X%z")
  }
  
    
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
    
    #httr2::req_dry_run(req)
    
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
#' @param src character, the UUID of the source (i.e. the tag ID)
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
#' @param additional_cols names of columns to store in data in ER. If `NULL`
#'   (default), the existing attributes in the "additional" field are preserved
#'   (i.e. not PATCHed). If provided, the "additional" field in ER's Observation
#'   will be fully overwritten with the specified attributes and their values —
#'   **use with caution**.
#'   
#'  @details
#' General assumptions and scope:
#'  - location columns must be named "lat"/"lon"
#'  - Currently this Patch function is (deliberately) limited to only update 
#'  the following Obs fields: "locations", "additional" and "exclusion_flags"
#'   
patch_obs <- function(data,
                      additional_cols = NULL,
                      api_base_url, 
                      token){
  
  # Input Validation -----------------------------------------------------------
  if(nrow(data) == 0){
    logger.warn("  |- No observations to PATCH - skipping PATCHing step.")
    return(NULL)
  }
  
  # if ("er_obs_id" %!in% names(data)) {
  #   cli::cli_abort("{.arg data} must contain column {.val er_obs_id} to match observations to existing records.")
  # }
  
  req_cols <- c(additional_cols, "er_obs_id", "cluster_status", "lat", "lon")
  miss_cols <- req_cols[req_cols %!in% names(data)]
  if (length(miss_cols) > 0) {
    cli::cli_abort("{.arg data} is missing the following required columns: {.val {miss_cols}}.")
  }
  
  # Pre-Processing --------------------------------------------------------
  
  ## reformat columns with special classes, for JSON serialization
  data <- data |> 
    data.frame() |> 
    dplyr::mutate(
      # date-time cols formatted as ISO 8601 strings
      dplyr::across(dplyr::where(~inherits(.x, "POSIXt")), \(x) format(x, "%Y-%m-%d %X%z")),
      # convert any columns of class integer64 to string
      dplyr::across(dplyr::where(~inherits(.x, "integer64")), as.character),
      #drop units
      dplyr::across(dplyr::where(~inherits(.x, "units")), .fns = \(x) units::set_units(x, NULL))
    )
  
  # Perform PATCH requests (1 per observation) ----------------------------
  logger.info("PATCHing requests for observation updates...")

  results <- data |> 
    dplyr::group_split(er_obs_id) |> 
    purrr::map_int(
      function(obs){
        api_endpnt <- file.path(api_base_url, "observation", obs$er_obs_id)
        
        body_list <- list(
          locations = list(lat = obs[["lat"]], lon = obs[["lon"]]),
          exclusion_flags = dplyr::if_else(obs[["cluster_status"]] == "ACTIVE", as.integer64(active_flag), as.integer64(0), missing = as.integer64(0)),
          additional = as.list(obs[additional_cols])
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
      .progress = list(format = "Updating observation {pb_current}/{pb_total} [{pb_rate}] | {cli::pb_eta_str}")
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



# ////////////////////////////////////////////////////////////////////////////////
#'  CLUSTER MATCH FUNCTION
#' This function is the workhorse for matching master clusters to their updated counterparts. 
#' The updating of IDs is not handled here (yet).
#' 
#' @param hist_dt an `<sf>` object, the historic observation dataset from ER,
#'   annotated with clusters. Must contain the following columns:
#'    - `"cluster_uuid"`: providing the universally unique ID of a cluster, as stored in
#'    the master observation data
#'    - `"recorded_at"`: the date-time at which the observation was recorded
#' @param new_dt as `<move2>` object, the newest processed data from upstream App, i.e. the
#'   Apps input data, annotated with clusters.
#' @param cluster_id_col character, the name of the column in `new_dt` containing the
#'   cluster ID annotations.
#' @param timestamp_col character, name of the column in `new_dt` that contains the
#'   recording timestamps of each observation.
#' @param days_thresh integer, the time difference between two cluster-intervals
#'   that must not be exceeded for these clusters to be merged.
#' @param dist_thresh a `<units>` object, the maximum distance (e.g. in
#'   meters) between two cluster centroids/medians for them to be 'matched'.
#' @param match_criteria character, whether to perform the merging based on
#'   centroids or geometric medians.
#' @param plot_results logical, if TRUE, cluster-matching plots will be
#'   generated as a diagnostic.
#'   
#' @return a list with elements:
#'   - `matched_master_data`: an `<sf>` object with the same nr of rows as 
#'   `hist_dt` with appended cluster-level info and matching cluster ID in the `new_dt`
#'   - `match_table`: a data frame with match information between the two datasets
#'   - `match_plot`: (optional) diagnostic plots with visualization of returned 
#'   cluster matching
#'      
match_sf_clusters <- function(hist_dt,
                              new_dt,
                              cluster_id_col,
                              timestamp_col,
                              days_thresh = 7,
                              dist_thresh = units::set_units(100, "metres"),
                              match_criteria = c("gmedian", "centroid"),
                              plot_results = FALSE
) {
  
  logger.info("Matching Spatial Clusters")
  
  # Input validation ----------------------
  
  # check object class
  if(!move2::mt_is_move2(new_dt)){
    cli::cli_abort("{.arg new_dt} must be a {.cls move2} object, not {.cls {class(new_dt)}}.")
  }
  
  if(!inherits(hist_dt, "sf")){
    cli::cli_abort("{.arg hist_dt} must be a {.cls sf} object, not {.cls {class(hist_dt)}}.")
  }
  
  # check hist_st and new_dt dataset is non-null and non-empty
  if (is.null(new_dt) || nrow(new_dt) == 0) {
    cli::cli_abort(c(
      "{.arg new_dt} must a populated dataset.",
      x = "Provided object is `NULL` or empty.")
    )
  }
  
  # If the old clusters are null or empty, return the new clusters 
  if (is.null(hist_dt) || nrow(hist_dt) == 0) {
    cli::cli_abort(c(
      "{.arg hist_dt} must a populated dataset.",
      x = "Provided object is `NULL` or empty.")
    )
    # return(new)
  }
  
  # Check that match_criteria is valid
  match_criteria <- rlang::arg_match(match_criteria)

  # Check that the cluster_id_col exists in new data
  if (!cluster_id_col %in% names(new_dt)) {
    cli::cli_abort("Column {.val {cluster_id_col}} must be present in {.arg new_dt}.")
  }
  
  # Check that the timestamp_col exists in new data and it's of class POSIXt
  if (!timestamp_col %in% names(new_dt)) {
    cli::cli_abort("Column {.val {timestamp_col}} must be present in {.arg new_dt}.")
  } else{
    if(!inherits(new_dt[[timestamp_col]], "POSIXt")){
      cli::cli_abort("Values in column {.val {timestamp_col}} of {.arg new_dt} must inherit from class {.cls POSIXt}.")
    }
  }
  
  
  if ("cluster_uuid" %!in% names(hist_dt)) {
    cli::cli_abort("Column {.val cluster_uuid} must be present in {.arg hist_dt}.")
  }
  
  # Check that column "recorded_at" exists in historic data and it's of class POSIXt
  if ("recorded_at" %!in% names(hist_dt)) {
    cli::cli_abort("Column {.val recorded_at} must be present in {.arg hist_dt}.")
  }else{
    if(!inherits(hist_dt[["recorded_at"]], "POSIXt")){
      cli::cli_abort("Values in column {.val recorded_at} of {.arg hist_dt} must inherit from class {.cls POSIXt}.")
    }
  }
  
  if(!inherits(dist_thresh, "units")){
    cli::cli_abort("{.arg dist_thresh} must be a {.cls units} object.")
  }
  
  
  # Convert coord. system of old to new
  hist_dt <- sf::st_transform(hist_dt, sf::st_crs(new_dt))
  
  # Valid. Proceed
  logger.info(sprintf(
    "  |- Matching %d clusters in newest data to %d clusters in historical data.",
    length(unique(new_dt[[cluster_id_col]])), length(unique(hist_dt[["cluster_uuid"]]))
  ))
  
  logger.info(sprintf(
    "  |- Matching criteria: %s, threshold: %s%s.", match_criteria, as.character(dist_thresh), units::deparse_unit(dist_thresh)
  ))
  
  # Reformat data as necessary ---------------------
  # 1. force cluster IDs as character strings
  # 2. create a temporary columns with homogeneous timestamps (UTC)
  hist_dt <- hist_dt |> 
    dplyr::mutate(
      XTEMPCLUSTER = as.character(cluster_uuid),
      XTIMESTAMP = lubridate::with_tz(recorded_at, tz = "UTC")
    ) 
  
  new_dt <- new_dt |>
    dplyr::mutate(
      XTEMPCLUSTER = as.character(.data[[cluster_id_col]]),
      XTIMESTAMP = lubridate::with_tz(.data[[timestamp_col]], tz = "UTC")
    )

  # Determine centroids --------------------
  
  logger.info(sprintf(
    "  |- Calculating centroids for old and new clusters based on matching criteria: %s.", 
    match_criteria
  ))
  
  # If gmedian, calculate the geometric median
  if (match_criteria == "gmedian") {
    
    master_centroids <- hist_dt |> 
      dplyr::filter(!is.na(XTEMPCLUSTER)) |>
      dplyr::group_by(XTEMPCLUSTER) |> 
      dplyr::summarise(
        geometry = calcGMedianSF(geometry), .groups = "drop",
        starttime = min(XTIMESTAMP, na.rm = TRUE),
        endtime = max(XTIMESTAMP, na.rm = TRUE)
      )
    
    new_centroids <- new_dt |>
      dplyr::filter(!is.na(XTEMPCLUSTER)) |>
      dplyr::group_by(XTEMPCLUSTER) |> 
      dplyr::summarise(
        geometry = calcGMedianSF(geometry), .groups = "drop",
        starttime = min(XTIMESTAMP, na.rm = TRUE),
        endtime = max(XTIMESTAMP, na.rm = TRUE)
      )
    
  } else if (match_criteria == "centroid") {
    
    # If centroid, calculate the centroid
    master_centroids <- hist_dt |> 
      dplyr::filter(!is.na(XTEMPCLUSTER)) |>
      dplyr::group_by(XTEMPCLUSTER) |> 
      dplyr::summarise(
        geometry = sf::st_combine(geometry), .groups = "drop",
        starttime = min(XTIMESTAMP, na.rm = TRUE),
        endtime = max(XTIMESTAMP, na.rm = TRUE)
      ) |> 
      sf::st_centroid()
    
    new_centroids <- new_dt |>
      dplyr::filter(!is.na(XTEMPCLUSTER)) |>
      dplyr::group_by(XTEMPCLUSTER) |> 
      dplyr::summarise(
        geometry = sf::st_combine(geometry), .groups = "drop",
        starttime = min(XTIMESTAMP, na.rm = TRUE),
        endtime = max(XTIMESTAMP, na.rm = TRUE)
      ) |> 
      sf::st_centroid()
  }
  
  # Match centroids ------------------
  
  # Buffer the new centroids by the matching threshold
  new_centroids_buffered <- sf::st_buffer(new_centroids, dist = dist_thresh)
  
  # Now we want a data.frame of matches. Left column contains any master cluster ID, 
  # right column contains any matching new cluster ID, with multiple entries for 
  # multiple matches.
  matches <- sf::st_join(master_centroids, new_centroids_buffered, join = sf::st_intersects) |>
    data.frame() |> 
    dplyr::select(XTEMPCLUSTER.x, XTEMPCLUSTER.y,
                  starttime.x, endtime.x,
                  starttime.y, endtime.y
    ) |>
    dplyr::rename(
      master_cluster = XTEMPCLUSTER.x,
      master_start = starttime.x,
      master_end = endtime.x,
      new_cluster = XTEMPCLUSTER.y,
      new_start = starttime.y,
      new_end = endtime.y
    ) #|>
  # dplyr::filter(!is.na(master_cluster) & !is.na(new_cluster)) # Remove any rows where no match was found
  
  # If we have a valid matching threshold, remove any matches that are outside
  # the threshold
  if (!is.na(days_thresh) && days_thresh > 0) {
    time_matches <- matches |> 
      # We buffer the interval by half of days_thresh (rounded) and check overlap
      dplyr::mutate(
        master_interval = lubridate::interval(master_start - lubridate::days(floor(days_thresh/2)), master_end + lubridate::days(ceiling(days_thresh/2))),
        new_interval = lubridate::interval(new_start - lubridate::days(floor(days_thresh/2)), new_end + lubridate::days(ceiling(days_thresh/2))),
        VALID_TIME = lubridate::int_overlaps(master_interval, new_interval)
      ) |> 
      
      # If the time match is invalid, delete the entry
      dplyr::filter(!is.na(VALID_TIME) | VALID_TIME) |> 
      dplyr::select(master_cluster, new_cluster)
    
    logger.info(sprintf(
      "  |- Removing %d matches that do not meet the time threshold of %d days.",
      nrow(matches) - nrow(time_matches), days_thresh
    ))
    
    # And add a new entry for any master_clusters missing
    missing_clusters <- hist_dt$XTEMPCLUSTER[!hist_dt$XTEMPCLUSTER %in% time_matches$master_cluster] |> 
      unique() |> 
      # drop NA
      na.omit() |> 
      as.vector() 
    
    if (length(missing_clusters) > 0) {
      missing_clusters_df <- data.frame(
        master_cluster = missing_clusters,
        new_cluster = NA_character_
      )
      matches <- dplyr::bind_rows(time_matches, missing_clusters_df)
    } # debug
  } else {
    # If not handling times, just get down to the essential columns
    matches <- matches |> 
      dplyr::select(master_cluster, new_cluster) |> 
      dplyr::distinct() 
  }
  
  
  # Filter to double-matches, where an old cluster is matched to multiple new clusters
  double_matches <- matches |>
    dplyr::group_by(master_cluster) |>
    dplyr::filter(dplyr::n() > 1
    ) |>
    dplyr::ungroup()
  
  direct_matches <- matches |>
    dplyr::filter(!(master_cluster %in% double_matches$master_cluster)) 
  
  nonmatches <- matches |> 
    dplyr::filter(!is.na(master_cluster) & is.na(new_cluster)) |> 
    dplyr::pull(master_cluster) 
  
  logger.info(sprintf(
    "  |- Found %d master clusters that do not match any new clusters.", length(nonmatches)
  ))
  
  if (any(nonmatches %in% double_matches$master_cluster)) {
    cli::cli_abort("Some master clusters simultaneously have no matches and double matches. This is conflicting.")
  }
  
  logger.info(sprintf(
    "  |- Found %d matches, of which %d are double-matches.", nrow(matches), nrow(double_matches)
  ))
  
  # Performing match ---------------------
  
  # Resolve matches down to observation level 
  matched_old_obs <- hist_dt |> 
    dplyr::left_join(direct_matches, by = c("XTEMPCLUSTER" = "master_cluster")) |> 
    # Get the nearest ID for ALL clusters
    dplyr::mutate(
      nearest_new_cluster = new_centroids$XTEMPCLUSTER[sf::st_nearest_feature(geometry, new_centroids)]
    ) |> 
    # If the old cluster is in double_matches, reassociate the new cluster to nearest_new_cluster
    dplyr::mutate(
      new_cluster = ifelse(
        XTEMPCLUSTER %in% double_matches$master_cluster,
        nearest_new_cluster,
        new_cluster
      )
    ) |> 
    # If the old cluster is in nonmatches, we need to create a new temporary cluster ID
    dplyr::group_by(XTEMPCLUSTER) |> 
    
    # If the old cluster is in nonmatches, assign a new cluster ID generated for the group
    dplyr::mutate(
      new_cluster = ifelse(
        XTEMPCLUSTER %in% nonmatches,
        paste0("UNMATCHED.", dplyr::cur_group_id()),
        new_cluster
      )
    ) |>
    dplyr::ungroup() |> 
    
    dplyr::select(-all_of("nearest_new_cluster")) 
  
  # Create a lookup table for matches to output
  final_matches <- matched_old_obs |> 
    data.frame() |> 
    dplyr::select(c("XTEMPCLUSTER", "new_cluster")) |>
    dplyr::rename(
      master_cluster = XTEMPCLUSTER
    ) |>
    dplyr::mutate(
      # This is Partial if the old cluster is in double_matches,
      # otherwise it is Full
      `Match Type` = ifelse(master_cluster %in% double_matches$master_cluster, "Partial", "Full"),
      # If either cluster column is NA, overwrite `Match Type` as "No Match"
      # `Match Type` = ifelse(is.na(old_cluster) | is.na(new_cluster), "No Match", `Match Type`)
      `Match Type` = ifelse(is.na(new_cluster), "No Match", `Match Type`),
      # If the master cluster is nonmatched, mark it 
      `Match Type` = ifelse(master_cluster %in% nonmatches, "No Match", `Match Type`)
    ) |> 
    dplyr::distinct() # Ensure distinct matches
  
  # Add to final_matches any new clusters that have no previous match
  unmatched_new_clusters <- new_centroids |> 
    data.frame() |> 
    dplyr::filter(!XTEMPCLUSTER %in% final_matches$new_cluster) |> 
    dplyr::mutate(
      master_cluster = NA_character_,
      `Match Type` = "No Match"
    ) |> 
    dplyr::select(master_cluster, new_cluster = XTEMPCLUSTER, `Match Type`)
  
  logger.info(sprintf(
    "  |- Found %d unmatched new clusters. Adding to matchtable", nrow(unmatched_new_clusters)
  ))
  
  final_matches <- final_matches |>
    dplyr::bind_rows(unmatched_new_clusters) |>
    dplyr::filter(!is.na(master_cluster) | !is.na(new_cluster)) |> # to get rid of NA -> NA
    dplyr::distinct() # Ensure distinct matches
  
  
  # Give the cluster column back its original name
  outdata <- matched_old_obs |> 
    dplyr::mutate(
      !!cluster_id_col := new_cluster
    ) |> 
    dplyr::rename(master_cluster = XTEMPCLUSTER) |> 
    dplyr::select(
      -any_of(c("XTEMPCLUSTER", "XTIMESTAMP", "new_cluster"))
    )
  
  
  # Plots ------------------
  
  # If plot_results is TRUE, we want a plot of arrows 
  # originating from the old cluster 
  # and pointing to the new cluster
  if (plot_results) {
    
    print("Preparing output cluster-matching plots")
    
    # Create a plot of the old and new clusters
    old_coords <- master_centroids |> 
      cbind(st_coordinates(master_centroids)) |> 
      as.data.frame() |> 
      dplyr::rename(
        master_cluster = XTEMPCLUSTER,
        Xold = X, Yold = Y
      ) |> 
      dplyr::select(
        all_of(c("master_cluster", "Xold", "Yold"))
      )
    new_coords <- new_centroids |> 
      cbind(sf::st_coordinates(new_centroids)) |> 
      as.data.frame() |> 
      dplyr::rename(
        new_cluster = XTEMPCLUSTER,
        Xnew = X, Ynew = Y
      ) |> 
      dplyr::select(
        all_of(c("new_cluster", "Xnew", "Ynew"))
      )
    
    # Join these to the final_matches
    plot_matches <- final_matches |> 
      dplyr::left_join(old_coords, by = "master_cluster") |> 
      dplyr::left_join(new_coords, by = "new_cluster")
    
    # And plot out
    # Top-level cluster plotting
    matchplot <- ggplot2::ggplot() + 
      ggplot2::theme_bw() +
      ggplot2::geom_sf(data = new_centroids_buffered |> dplyr::filter(XTEMPCLUSTER %in% plot_matches$new_cluster), fill = NA, alpha = 0.15, linewidth = 0.1,
                       ggplot2::aes(color = "New Cluster", linetype = "Connection Radius")
      ) +
      ggplot2::geom_segment(data = plot_matches |> dplyr::filter(!is.na(new_cluster) & !is.na(master_cluster)),
                   ggplot2::aes(x = Xold, y = Yold, xend = Xnew, yend = Ynew, linetype = `Match Type`), linewidth = 0.6,
                   arrow = ggplot2::arrow(length = ggplot2::unit(0.2, "cm")), size = 0.5) +
      ggplot2::geom_label(data = dplyr::filter(plot_matches, !is.na(master_cluster)), 
                          ggplot2::aes(x = Xold, y = Yold, label = master_cluster, color = "Master Cluster", fill = `Match Type`), 
                          size = 3) +
      ggplot2::geom_label(data = dplyr::filter(plot_matches, !is.na(new_cluster)),
                         ggplot2::aes(x = Xnew, y = Ynew, label = new_cluster, color = "New Cluster",fill = `Match Type`), 
                         size = 3) +
      ggplot2::scale_color_manual(
        name = "Cluster Type",
        values = c("Match" = "black", "Master Cluster" = "blue", "New Cluster" = "red")) +
      ggplot2::scale_fill_manual(
        name = "Unmatched Clusters",
        values = c("Full" = "white", "Partial" = "white", "No Match" = "yellow")) +
      ggplot2::scale_linetype_manual(
        name = "Match Type",
        values = c("Full" = "solid", "Partial" = "dashed", "Connection Radius" = "dotted")) +
      ggplot2::xlab("X") + ggplot2::ylab("Y") +
      ggplot2::ggtitle("Cluster association map")
    # coord_fixed()
    
    # Point-level cluster plotting
    prep_pts <- outdata |> 
      cbind(sf::st_coordinates(outdata)) |> 
      data.frame() |> 
      dplyr::rename(
        XTEMPCLUSTER = !!rlang::sym(cluster_id_col),
        pointX = X, pointY = Y) |> 
      # dplyr::filter(!is.na(XTEMPCLUSTER) & XTEMPCLUSTER != 0) |> 
      dplyr::left_join(
        new_centroids |>
          cbind(st_coordinates(new_centroids)) |>
          data.frame() |>
          rename(clusterX = X, clusterY = Y),
        by = "XTEMPCLUSTER"
      ) |> 
      dplyr::left_join(
        master_centroids |>
          cbind(st_coordinates(master_centroids)) |>
          data.frame() |>
          dplyr::filter(!is.na(XTEMPCLUSTER) & XTEMPCLUSTER != 0) |> 
          rename(clusterXold = X, clusterYold = Y,
                 master_cluster = XTEMPCLUSTER
          ),
        by = "master_cluster"
      ) 
    
    pointplot <- ggplot2::ggplot() + 
      ggplot2::theme_bw() +
      ggplot2::geom_segment(
        data = prep_pts |> dplyr::filter(!is.na(master_cluster)), 
        ggplot2::aes(
          x = pointX, y = pointY,
          xend = clusterXold, yend = clusterYold, color = "Master"
        ),
        linetype = "dashed"
      ) +
      ggplot2::geom_segment(
        data = prep_pts |> dplyr::filter(!is.na(master_cluster) & !is.na(XTEMPCLUSTER)),
        ggplot2::aes(
          x = pointX, y = pointY,
          xend = clusterX, yend = clusterY, color = "New"
        )
      ) +
      ggplot2::geom_point(
        data = prep_pts |> dplyr::filter(is.na(XTEMPCLUSTER)), 
        ggplot2::aes(x = pointX, y = pointY, color = "No Cluster"),
        shape = 4
      ) +
      ggplot2::scale_color_manual(
        name = "Cluster Type",
        values = c("Master" = "blue", "New" = "red", "No Cluster" = "black")
      ) +
      ggplot2::coord_fixed() + 
      ggplot2::xlab("X") + ggplot2::ylab("Y") +
      ggplot2::ggtitle("Master Clusters [mapped to new centroids]")
    
    allplot <- cowplot::plot_grid(
      matchplot, pointplot,
      ncol = 2, 
      labels = c("A", "B"),
      label_size = 12
    )
    
  } else {allplot <- NULL}
  
  logger.info(sprintf(
    "  |- Matched %d clusters between old data to new data.", nrow(final_matches)
  ))
  
  # Return list 
  return(
    list(matched_master_data = outdata, match_table = final_matches, match_plot = allplot)
  )
  
}


# ///////////////////////////////////////////////////////////////////////////////
calcGMedianSF <- function(data) {
  if (st_geometry_type(data[1,]) == "POINT") {
    med <- st_coordinates(data)
    
    med <- Gmedian::Weiszfeld(st_coordinates(data))$median %>% as.data.frame() %>%
      rename(x = V1, y = V2) %>%
      st_as_sf(coords = c("x", "y"), crs = st_crs(data)) %>%
      st_geometry()
  }
  
  if (st_geometry_type(data[1,]) == "MULTIPOINT") {
    med <- data %>% 
      st_coordinates() %>%
      as.data.frame() %>%
      group_by(L1) %>%
      group_map( ~st_point(Gmedian::Weiszfeld(.)$median) ) |> 
      st_as_sfc(crs = st_crs(data))
  }
  return(med)
}



# ////////////////////////////////////////////////////////////////////////////////
#' Cluster-aware Merging of Historic and New observations 
#'   
#' @param matched_hist_dt `<sf>`, the 'master' dataset (from ER) with clusters
#'   matched to `new_dt` by `match_sf_clusters()`. 
#' @param new_dt `<move2>`, the 'new' dataset (from MA) with some cluster matching
#'   those in `matched_hist_dt`. It must contain columns `"lat"` and `"lon"`.
#' @param cluster_id_col character, the name of the column in `matched_hist_dt`
#'   and `new_dt` containing the cluster ID annotations.
#' @param timestamp_col character, name of the column in `new_dt` that contains the
#'   recording timestamps of each observation.
#' @param active_days_thresh integer, if this many days have passed between a
#'   cluster's most recent observation and the overall most recent observation,
#'   the cluster will be marked as CLOSED. Otherwise, it will be ACTIVE.
#'   Non-clustered obs will be NA.
#'        
#' @details#' 
#' In addition merging the two datasets, it performs the following tasks:
#'  * Clusters status update ("ACTIVE" vs "CLOSED")
#'  * UUID issuing for new clusters, while ensuring stable UUIDs for historical clusters
#'  * Annotate merged data into PATCH and POST observations ready to be sent to the ER server. 
#'  
#' @return
#' An `<sf>` object with updated cluster information and API request requirements
#'  
merge_and_update <- function(matched_hist_dt,
                             new_dt,
                             cluster_id_col,
                             timestamp_col,
                             store_cols,
                             active_days_thresh = 14
) {
  
  # Input validation  --------------------------
  # check object class
  if(!move2::mt_is_move2(new_dt)){
    cli::cli_abort("{.arg new_dt} must be a {.cls move2} object, not {.cls {class(new_dt)}}.")
  }

  ## Check that required columns are present
  new_req_cols <- c(cluster_id_col, timestamp_col, "lon", "lat")
  new_miss_cols <- new_req_cols[new_req_cols %!in% names(new_dt)]
  if (length(new_miss_cols) > 0) {
    cli::cli_abort("{.arg new_dt} is missing the following required columns: {.val {new_miss_cols}}.")
  }
  
  hist_req_cols <- c(
    cluster_id_col, "cluster_uuid", "subject_name", "manufacturer_id", "recorded_at", "er_obs_id", 
    "er_source_id", "lon", "lat"
  )
  hist_miss_cols <- hist_req_cols[hist_req_cols %!in% names(matched_hist_dt)]
  if (length(hist_miss_cols) > 0) {
    cli::cli_abort("{.arg matched_hist_dt} is missing the following required columns: {.val {hist_miss_cols}}.")
  }
  
  # Initialise -----------------------
  logger.info("Performing data merging")
  logger.info(sprintf("  |- Sorting %d masterobs and %d newobs", nrow(matched_hist_dt), nrow(new_dt)))
  
  # Ensure that at least one cluster ID is present in both datasets
  # to ensure we have some form of consistency
  if (length(intersect(matched_hist_dt[[cluster_id_col]] |> na.omit() |> as.vector(),
                       new_dt[[cluster_id_col]] |> na.omit() |> as.vector()
  )) == 0) {
    logger.warn("  |- No common cluster IDs found between historic and new data. Check that matching has been effective")
  }
  
  
  # Homogenise datasets for binding ------------------------
  # i.e. coerce column formatting of historic dataset to new data
  
  ## Bring subject- tag- study- level IDs attributes from to events-table
  new_dt <- new_dt |>
    move2::mt_as_event_attribute(dplyr::any_of(mv2_track_cols)) |> 
    # bind track ID
    dplyr::mutate(track_id = move2::mt_track_id(new_dt))
  
  ## Rename ER-based key "Observation" columns in historic dataset 
  matched_hist_dt <- matched_hist_dt |> 
    dplyr::rename(
      individual_local_identifier = subject_name, 
      tag_id = manufacturer_id,
      {{timestamp_col}} := recorded_at
    )
  
  
  ## Introspect the class/type of mutual columns
  mutual_cols <- intersect(names(new_dt), names(matched_hist_dt))
  
  mutual_cols_prof <- purrr::map(mutual_cols, function(col){
    x <- new_dt[[col]]
    data.frame(
      col_name = col,
      cls = class(x)[[1]], # top-level class
      units = ifelse(inherits(x, "units"), units::deparse_unit(x), NA)
    )
  }) |> 
    purrr::list_rbind()
  
  # Coerce classes of columns in historic data. Currently only dealing with 
  # `<units>`, `<POSIXT`> and `<integer64>`
  mutual_cols_prof |> 
    purrr::pwalk(function(col_name, cls, units){
      if (cls == "units"){
        matched_hist_dt[[col_name]] <<- units::set_units(matched_hist_dt[[col_name]], units, mode = "standard")
      } else if (cls == "POSIXct"){
        matched_hist_dt[[col_name]] <<- lubridate::ymd_hms(matched_hist_dt[[col_name]], tz = "UTC")
      } else if (cls == "integer64") {
        matched_hist_dt[[col_name]] <<- bit64::as.integer64(matched_hist_dt[[col_name]])
      }
    })
  
  ## Add cluster provenance
  matched_hist_dt <- matched_hist_dt |>
    dplyr::mutate(
      CLUSTERORIGIN = "MASTER",
      {{cluster_id_col}} := as.character(.data[[cluster_id_col]])
    )
  
  new_dt <- new_dt |>
    dplyr::mutate(
      CLUSTERORIGIN = "NEW",
      {{cluster_id_col}} := as.character(.data[[cluster_id_col]])
    )
  
  
  # Row Bind Datasets ----------------------------------------------------------
  ## Note: row bind returns an <sf> object - i.e. the track-table is dropped out
  alldata <- dplyr::bind_rows(matched_hist_dt, new_dt) |> 
    dplyr::mutate(
      # Create temporary clusters to store essential info
      XTEMPCLUSTERCOL = .data[[cluster_id_col]],
      XTEMPTIMECOL = .data[[timestamp_col]],
      XTEMPIDCOL = individual_local_identifier
    )
  
  # Group by timestamp, longitude/latitude and animal ID to identify duplicates
  alldata <- alldata |> 
    dplyr::group_by(XTEMPTIMECOL, XTEMPIDCOL, lon, lat) |>
    dplyr::mutate(
      # Create a unique identifier for each group
      XTEMPUNIQUEID = dplyr::cur_group_id(),
      # Count the number of observations in each group
      XTEMPCOUNT = dplyr::n()
    )
  
  # If any counts are 3, we have a problem. Flag a warning
  if (any(alldata$XTEMPCOUNT > 2)) {
    logger.warn(sprintf("  |- Found %d groups with more than 2 observations. This may indicate an issue with the data.", sum(alldata$XTEMPCOUNT > 2)))
  }
  
  logger.info(sprintf("  |- Found %d observations, of which %d have duplicates", nrow(alldata), sum(alldata$XTEMPCOUNT > 1)))
  
  # Merge data ----------------------------------------------------------
  
  # Whenever COUNT > 1, we have a duplicate observation which consists of an
  # observation from ER and an observation from MoveApps. The ER observation
  # will have a observation_id, which we want to retain. However, we want to
  # retain ALL other data in most recent data to the ER observation, as it may
  # have been updated.
  merged_dt <- alldata |> 
    dplyr::group_by(XTEMPUNIQUEID) |> 
    dplyr::mutate(
      # Duplicate the non-NA observation_id and source_id across the whole group
      er_source_id = first(er_source_id[!is.na(er_source_id)]),
      er_obs_id = first(er_obs_id[!is.na(er_obs_id)]),
      cluster_uuid = first(cluster_uuid[!is.na(cluster_uuid)]),
      # flag for unchanged columns in `store_cols` so that they are excluded from PATCHing
      STORECOLSMODIFIED = dplyr::n_distinct(across(any_of(store_cols))) > 1,
      cluster_merge_status = get_cluster_merge_status(XTEMPCLUSTERCOL, CLUSTERORIGIN)
    ) |> 
    dplyr::filter(                        
      CLUSTERORIGIN == "NEW" | (CLUSTERORIGIN == "MASTER" & XTEMPCOUNT == 1)
    ) |> 
    dplyr::arrange(XTEMPIDCOL, XTEMPTIMECOL)
  
  
  # Classify Cluster Status -------------------------------------
  
  # Now we want to rewrite the cluster_status column to determine whether a cluster is 
  # ACTIVE or CLOSED. This will be determined by the most recent timestamp in all clusters:
  # if it was within active_days_thresh, then the cluster is ACTIVE, otherwise it is CLOSED.
  most_recent_obs <- new_dt |> 
    dplyr::ungroup() |> 
    dplyr::pull(.data[[timestamp_col]]) |>
    max(na.rm = TRUE)
  
  merged_dt <- merged_dt |> 
    dplyr::group_by(
      XTEMPCLUSTERCOL
    ) |> 
    dplyr::mutate(
      # Determine the most recent timestamp in the cluster
      MAXTIMESTAMP = max(XTEMPTIMECOL, na.rm = TRUE),
      # Determine if the cluster is ACTIVE or CLOSED
      cluster_status = ifelse(
        MAXTIMESTAMP >= (most_recent_obs - lubridate::days(active_days_thresh)),
        "ACTIVE",
        "CLOSED"
      ),
      # But if the cluster is NA, set it to NA
      cluster_status = ifelse(is.na(XTEMPCLUSTERCOL), NA, cluster_status)
    ) |> 
    dplyr::ungroup()
  
  
  # Handle Cluster UUIDs  -------------------------------------------
  
  ## Count number of unique non-NA clusters
  clusters_hist <- merged_dt |> 
    data.frame() |> 
    count(cluster_uuid) |> 
    dplyr::filter(!is.na(cluster_uuid))
  
  ## Issue UUIDs to freshly formed clusters
  cluster_uuids_map <- merged_dt |> 
    data.frame() |> 
    dplyr::filter(!is.na(XTEMPCLUSTERCOL)) |> 
    distinct(XTEMPCLUSTERCOL, cluster_uuid) |> 
    mutate(
      cluster_uuid = ifelse(
        all(is.na(cluster_uuid)), 
        paste(
          #ids::sentence(style = "camel"), 
          ids::adjective_animal(n_adjectives = 2, style = "camel"),
          format(Sys.time(), "%Y%m%d-%H%M%S"), 
          sep = "-"),
        #ids::uuid(use_time = TRUE), 
        cluster_uuid),
      .by = XTEMPCLUSTERCOL
    ) |> 
    distinct()
  
  ## Update cluster UUIDs and their earliest points in merged data
  merged_dt <- merged_dt |> 
    # drop "outdated" cluster UUID column
    dplyr::select(-cluster_uuid) |>
    # merge updated cluster UUID column
    left_join(cluster_uuids_map, by = "XTEMPCLUSTERCOL")
    
  ## Run Checks and log summaries
  clusters_merge <- merged_dt |> 
    data.frame() |> 
    count(cluster_uuid) |> 
    dplyr::filter(!is.na(cluster_uuid))

  if(nrow(clusters_merge) < nrow(clusters_hist)){
    cli::cli_abort(c(
      "Unexpectedly low number of clusters in merged data.",
      x = "Merged data contains fewer clusters than the historical dataset.",
      i = "This suggests some historical clusters may have been inadvertently dropped during processing."
    ))
  }
  
  merge_summ <- dplyr::full_join(clusters_hist, clusters_merge, by = "cluster_uuid") |> 
    dplyr::mutate(diff = n.y - n.x)
  
  logger.info(sprintf(
    "  |- Cluster-level merging summary:
           * Historical Clusters: Expanded: %d | Shrunk: %d | Unchanged: %d
           * New Clusters: %d",
    sum(merge_summ$diff > 0, na.rm = TRUE), 
    sum(merge_summ$diff < 0, na.rm = TRUE),
    sum(merge_summ$diff == 0, na.rm = TRUE),
    sum(is.na(merge_summ$n.x))
  ))
  
  
  # Annotate patching and posting ----------------------------------------
  
  # We now want to annotate data into PATCH and POST data for API processing. 
  # - POSTable obs will be those with an NA as observation_id.
  # - PATCHable obs will be those that have changed their cluster membership status 
  #   snice previous run or whose `store_cols` have changed
  merged_dt <- merged_dt |> 
    dplyr::mutate(
      request_type = dplyr::case_when(
        is.na(er_obs_id) ~ "POST",
        cluster_merge_status %in% c("RECRUITED", "TRANSFERRED", "DROPPED") ~ "PATCH",
        !is.na(er_obs_id) & STORECOLSMODIFIED ~ "PATCH",
        .default = NA_character_
      )
    )
  
  logger.info(glue::glue_collapse(c(
    "  |- Observation-level cluster membership status:", 
    glue::glue("           * {names(x)}: {x}", x = table(merged_dt$cluster_merge_status))), 
    sep = "\n"
  ))
  
  logger.info(sprintf(
    "  |- Observation-level API request status: 
           * PATCHable: %d
           * POSTable: %d 
           * Unchanged: %d",
    sum(merged_dt$request_type == "PATCH", na.rm = TRUE), 
    sum(merged_dt$request_type == "POST", na.rm = TRUE),
    sum(is.na(merged_dt$request_type))
  ))
  
  
  # Tidy-up and output ----------------------------------------
  merged_dt <- merged_dt |> 
    dplyr::select(-any_of(c(
      "MAXTIMESTAMP", "XTEMPCOUNT", "XTEMPUNIQUEID", "XTEMPCLUSTERCOL", 
      "XTEMPTIMECOL", "XTEMPIDCOL", "STORECOLSMODIFIED", "CLUSTERORIGIN"
    )))
  
  merged_dt
}
 


# /////////////////////////////////////////////////////////////////////////////
# Other short utility helpers

#' check if vector is parseable into date-time (POSIXt)
is_dttm_parseable <- function(x) any(!is.na(lubridate::ymd_hms(x, quiet = TRUE)))



# helper to classify the cluster merging status observation based on clusters
# IDs. To be used specifically after binding the historic an the new datasets.
# `x` is a character vector
get_cluster_merge_status <- function(cluster, source){
  
  out <- if(length(cluster) == 1){
    # single entry, i.e. only present in either hist or new dataset
    if(is.na(cluster)) {
      # no cluster annotation, so unclustered
      "NONCLUSTERED"
    } else if (source == "MASTER"){
      # in historical data, so no change
      "RETAINED"
    } else if (source == "NEW"){
      # in new data, thus added to existent cluster or a founding member of new cluster 
      "ENTRANT"
    }
    
  } else if(length(cluster) == 2){  
    # duplicate, i.e. observation present in hist and new datasets. Assigned
    # status denote type of change since previous run
    if(is.na(cluster[1]) && is.na(cluster[2])){
      # obs remains unclustered
      "NONCLUSTERED"
    } else if (is.na(cluster[1]) && !is.na(cluster[2])){
      # unclustered obs recruited to a cluster
      "RECRUITED"
    } else if(!is.na(cluster[1]) && is.na(cluster[2])){
      # clustered obs became unclustered
      "DROPPED"
    } else if(cluster[1] !=  cluster[2]){
      # clustered obs changed affiliation
      "TRANSFERRED"
    } else if(cluster[1] ==  cluster[2]){
      # clustered obs remains in same cluster
      "RETAINED"
    } else{
      # undetermined status
      NA_character_
    }
  } else {
    # more than two entries for the obs, which is unexpected 
    # returning NA for the moment, but might raise an error/warning in future
    NA_character_
  } 
  
  out
}