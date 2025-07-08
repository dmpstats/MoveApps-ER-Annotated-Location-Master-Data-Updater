# DEPRECATED DUE TO BY-PRODUCT OF USING EXCLUSIONS FLAGS TO TAG OBSERCATIONS IN
# ACTIVE CLUSTERS. THESE OBSERVATIONS GET HIDDEN FROM MAP DISPLAY

# /////////////////////////////////////////////////////////////////////////////////
#' Send new observations to EarthRanger
#' 
#' @param post_dt a data.frame, containing new observations to be pushed to
#'   EarthRanger.
#' @param tm_id_col character, name of the column in `post_dt` that contains the
#'   timestamps of each observation.
#' @param trk_id_col character, the name of the column in `post_dt` that
#'   identifies the associated track ID for each observation.
#' @param store_cols character vector, Names of additional columns in `post_dt`
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

send_new_obs_old <- function(post_dt, tm_id_col, store_cols, api_base_url, token){
  
  ### ensure listed store_cols are in data
  missing_store_cols <- store_cols %!in% names(post_dt)
  if(sum(missing_store_cols) > 0){
    cli::cli_abort("Columns {.val {store_cols[missing_store_cols]}} not found in {.arg post_dt}.")
  }
  
  # 1. Post all attributes except cluster_status ----- 
  # 
  # currently is not possible to include  exclusion flag in RA requests. ER
  # (Chris) promised this functionality could be available soon
  
  posting_dttm <- lubridate::now() - lubridate::seconds(60)
  ra_post_obs(
    data = post_dt, 
    tm_id_col = tm_id_col, 
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
      recorded_at = lubridate::ymd_hms(recorded_at, tz = "UTC"),
      .keep = "unused"
    ) |> 
    dplyr::select(id, recorded_at, lat, lon)
  
  # 3. Re-merge  -----------------------
  # Link cluster_status (aka exclusion_flag) to each observation IDs
  obs_excl_flags <- dplyr::right_join(
    post_dt, 
    er_obs, 
    by = dplyr::join_by({{tm_id_col}} == "recorded_at", "lat", "lon")
  ) |> 
    dplyr::mutate(
      exclusion_flags = dplyr::if_else(cluster_status == "ACTIVE", 8, 0, missing = 0),
      er_obs_id = id,
      .keep = "unused"
    ) |> 
    dplyr::select(er_obs_id, exclusion_flags) 
  
  # 4. Patch observations with latest exclusion_flag -------------------------
  patch_obs(obs_excl_flags, api_base_url = api_base_url, token = token)
  
}
