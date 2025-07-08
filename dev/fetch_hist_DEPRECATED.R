
# ////////////////////////////////////////////////////////////////////////////////////
#' Download a subset of historic observations from master dataset stored in ER
#' 
#' @param api_base_url `<character>`, the base URL of the API endpoint used to fetch
#'   historical data (e.g., `"https://api.example.org/v1"`).
#' @param token `<character>`, a valid authentication token used to authorize the
#'   request.
#' @param unclust_min_date `<POSIXt/POSIXct/POSIXlt>`, the start date-time for filtering
#'   un-clustered observations (inclusive). If `NULL` (default, no lower date-time bound is
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
#'      1. **ALL** observations tagged with clusters that are currently active 
#'      (`filter` == 8)
#'      2. For a given time-window: observations tagged with closed clusters AND
#'      un-clustered observations
#' - Output: attributes are renamed to ensure compatibility with original 
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
    filter = 8,
    min_date = NULL, 
    max_date = max_date, 
    created_after = NULL,
    include_details = include_details,
    page_size = page_size
  )
  
  
  # un-clustered and closed observations from min_date to max_date
  obs_uncluster <- get_obs(
    api_base_url = api_base_url, 
    token = token, 
    filter = 0,
    min_date = min_date, 
    max_date = max_date, 
    created_after = NULL,
    include_details = include_details,
    page_size = page_size
  ) 
  
  # safeguard for the very first call to ER, when there is still no data stored
  # in the expected format. So only filter out obs in "closed" clusters when
  # "cluster_uuid" is available
  if("cluster_uuid" %in% names(obs_uncluster)){
    # keep only un-clustered observations (i.e. with no cluster UUID annotated)
    obs_uncluster <- dplyr::filter(obs_uncluster, is.na(cluster_uuid))
  }
  # if("cluster_status" %in% names(obs_uncluster)){
  #   obs_uncluster <- dplyr::filter(obs_uncluster, is.na(cluster_status))
  # }
  
  # stack-up the two datasets
  obs <- dplyr::bind_rows(obs_cluster_actv, obs_uncluster)
  
  
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