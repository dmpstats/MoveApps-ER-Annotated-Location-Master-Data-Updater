

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