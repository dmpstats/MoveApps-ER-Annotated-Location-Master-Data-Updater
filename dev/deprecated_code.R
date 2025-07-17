

# post_new_obs <- function(data, api_base_url, token, provider_key = "moveapps_ann_locs", store_cols){
#   
#   if(!move2::mt_is_move2(data)){
#     msg <- "{.arg new_obs} must be a {.cls move2} object"
#     logger.fatal(cli::cli_text(msg))
#     cli::cli_abort(msg)
#   }
#   
#   tm_id_col <- mt_time_column(data)
#   trk_id_col <- mt_track_id_column(data)
#   
#   # Manipulate data for ER's POSTing:
#   #   - add additional required cols
#   #   - format cols for json serialization
#   #   - coerce as tibble (i.e. drop move2 and sf classes)
#   data <- data |> 
#     move2::mt_as_event_attribute(tag_id, individual_local_identifier, individual_id) |> 
#     #sf::st_drop_geometry() |> 
#     as.data.frame() |> 
#     dplyr::mutate(
#       # date-time cols formatted as ISO 8601 strings
#       #dplyr::across(dplyr::where(~inherits(.x, "POSIXt")), \(x) format(x, "%Y-%m-%d %X%z")),
#       dplyr::across(dplyr::where(~inherits(.x, "POSIXt")), \(x) format(x, "%Y-%m-%dT%H:%M:%SZ")),
#       ## convert any columns of class integer64 to string
#       dplyr::across(dplyr::where(~inherits(.x, "integer64")), as.character),
#       # drop unitsn
#       dplyr::across(dplyr::where(~inherits(.x, "units")), .fns = \(x) units::set_units(x, NULL)),
#       # add track_id info
#       track_id = .data[[trk_id_col]],
#       # encode active/closed clusters foe exclusion flagging
#       exclusion_flag = dplyr::case_when(
#         cluster_status == "active" ~ 8,
#         cluster_status == "closed" ~ 4,
#         is.na(cluster_status) ~ 0
#       )
#     )
#   
#   # bind "track_id" column to store as additional data
#   store_cols <- c(store_cols, "track_id")
#   
#   # append API's endpoint for request
#   api_endpnt <- paste0(api_base_url, "sensors/dasradioagent/", provider_key, "/status/")
#   
#   # post observations to ER via radio agent. These requests only take one
#   # observation each time
#   resp_status <- data |> 
#     # convert to list: 1 obs per element
#     split(sort(as.numeric(rownames(data)))) |> 
#     purrr::map(function(obs){
#       
#       req <- httr2::request(api_endpnt) |> 
#         httr2::req_auth_bearer_token(token) |> 
#         httr2::req_headers(
#           "Accept" = "application/json",
#           "Content-Type" = "application/json"
#         ) |> 
#         httr2::req_body_json(
#           list(
#             location = list(
#               lat = obs$lat, 
#               lon = obs$lon
#             ),
#             recorded_at = obs[[tm_id_col]],
#             manufacturer_id = obs$tag_id,
#             subject_name = obs$individual_local_identifier,
#             subject_type = "Wildlife",
#             subject_subtype = "Unassigned",
#             additional = as.list(obs[store_cols]),
#             exclusion_flags = 1 #as.integer(obs$exclusion_flag),
#           )
#         )
#       
#       browser()
#       #req |> httr2::req_dry_run()
#       
#       res <- req |> 
#         #httr2::req_error(~FALSE) |>
#         httr2::req_error(body = \(resp) httr2::resp_body_string(resp)) |> 
#         httr2::req_perform()
#     
#       data.frame(
#         status = res |> httr2::resp_status(),
#         msg = res |> httr2::resp_status_desc()
#       )
#       
#     }, 
#     .progress = list(format = "{cli::pb_spin} Posting Obs to ER: {pb_current}/{pb_total} [{pb_rate}] | {cli::pb_eta_str}") #"POSTing new observations to ER"
#     ) |> 
#     purrr::list_rbind()
#   
#   # summarize returned status
#   out <- resp_status |> 
#     group_by(status, msg) |> 
#     tally() |> 
#     ungroup() |> 
#     as.data.frame() |> 
#     capture.output() |> 
#     paste0(collapse = "\n")
#   
#   #out <- c("Response status tally ---\n", out, "\n")
#   out <- c("Tally of Request Status from ER ------\n", out, "\n")
#   
#   logger.info(paste0(out, collapse = ""))
#   #cli::cli_alert_info(out)
# 
#   invisible()
# }



# ## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 
# # append API's endpoint for request
# api_endpnt <- paste0(api_base_url, "sensors/dasradioagent/", provider_key, "/status/")
# 
# # post observations to ER via radio agent. These requests only take one
# # observation each time
# resp_status <- data |> 
#   # convert to list: 1 obs per element
#   split(sort(as.numeric(rownames(data)))) |> 
#   purrr::map(function(obs){
#     
#     req <- httr2::request(api_endpnt) |> 
#       httr2::req_auth_bearer_token(token) |> 
#       httr2::req_headers(
#         "Accept" = "application/json",
#         "Content-Type" = "application/json"
#       ) |> 
#       httr2::req_body_json(
#         list(
#           location = list(
#             lat = obs$lat, 
#             lon = obs$lon
#           ),
#           recorded_at = obs[[tm_id_col]],
#           manufacturer_id = obs$tag_id,
#           subject_name = obs$individual_local_identifier,
#           subject_type = "Wildlife",
#           subject_subtype = "Unassigned"
#         )
#       )
#     
#     browser()
#     #req |> httr2::req_dry_run()
#     
#     res <- req |> 
#       #httr2::req_error(~FALSE) |>
#       httr2::req_error(body = \(resp) httr2::resp_body_string(resp)) |> 
#       httr2::req_perform()
#     
#     data.frame(
#       status = res |> httr2::resp_status(),
#       msg = res |> httr2::resp_status_desc()
#     )
#     
#   }, 
#   .progress = list(format = "{cli::pb_spin} Posting Obs to ER: {pb_current}/{pb_total} [{pb_rate}] | {cli::pb_eta_str}") #"POSTing new observations to ER"
#   ) |> 
#   purrr::list_rbind()
# 
# # summarize returned status
# out <- resp_status |> 
#   group_by(status, msg) |> 
#   tally() |> 
#   ungroup() |> 
#   as.data.frame() |> 
#   capture.output() |> 
#   paste0(collapse = "\n")
# 
# #out <- c("Response status tally ---\n", out, "\n")
# out <- c("Tally of Request Status from ER ------\n", out, "\n")
# 
# logger.info(paste0(out, collapse = ""))
# #cli::cli_alert_info(out)
# 
# invisible()



# -----------------------------------------------------------------------
if(not_null(min_date)){
  if(!inherits(min_date, "POSIXt")) {
    cli::cli_abort("{.arg min_date} must be {.cls POSIXt}")
  }
}else{
  if(is.null(created_after)){
    ## If min_date is not provided, use 31 days from sys.time
    min_date <- Sys.time() - lubridate::days(31)  
  }
}

min_date <- if(not_null(min_date)) format(min_date, "%Y-%m-%d %X%z")





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

send_new_obs <- function(post_dt, tm_id_col, store_cols, api_base_url, token){
  
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






get_subject_ids <- function(subject_names, api_base_url, token){

  # get ER's subject_id based on subject_name
  subjects_api_endpnt <- file.path(api_base_url, "subjects")

  purrr::map(subject_names, function(name){
    #browser()
    req_subj <- httr2::request(subjects_api_endpnt) |>
      httr2::req_url_query(name = name) |>
      httr2::req_auth_bearer_token(token) |>
      httr2::req_headers("accept" = "application/json")

    #req_src |> req_dry_run()

    res <- req_subj |>
      httr2::req_perform() |>
      httr2::resp_body_json() |>
      purrr::pluck("data")

    if(length(res) > 0){
      data.frame(er_subject_name = name, er_subject_id = res[[1]]$id)
    } else NULL

  }) |>
    purrr::list_rbind()

}
