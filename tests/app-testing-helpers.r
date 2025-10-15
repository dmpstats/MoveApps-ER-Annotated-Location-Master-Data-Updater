require(httr2)
require(purrr)

`%notin%` <- Negate(`%in%`)

#////////////////////////////////////////////////////////////////////////////////
## helper to get app key to then retrieve app secrets 
get_app_key <- function() {
  
  require(rlang)
  
  key <- Sys.getenv("MOVEAPPS_APP_KEY")
  if (identical(key, "")) {
    rlang::abort(message = c(
      "No App key found",
      "x" = "You won't be able to proceed with the App testing. Sorry!", 
      "i" = "You can request the App key from the App developers for testing purposes (bruno@dmpstats.co.uk)",
      "i" = "Set the provided App key via usethis::edit_r_environ() using enviroment variable named 'MOVEAPPS_APP_KEY'"
    ))
  }
  key
}



# /////////////////////////////////////////////////////////////////////////////
## helper to set interactive testing of main App RFunction (e.g. in testthat 
## interactive mode, or on any given script)
##
set_interactive_app_testing <- function(){
  
  source("RFunction.R")
  source("src/common/logger.R")
  source("src/io/app_files.R")
  
  options(dplyr.width = Inf)
}


# /////////////////////////////////////////////////////////////////////////////
# ER API Helpers
delete_obs <- function(obs_ids, token){
  
  res <- purrr::map_dbl(obs_ids, function(id){
    
    api_endpnt <- file.path("https://standrews.dev.pamdas.org/api/v1.0/observation", id)
    
    req <- httr2::request(api_endpnt) |> 
      req_auth_bearer_token(token) |> 
      req_method("DELETE") |> 
      req_headers(
        "Accept" = "application/json",
        "Content-Type" = "application/json"
      )
    req |>  httr2::req_perform() |> httr2::resp_status()
  }, 
  .progress = TRUE)
  
  cli::cli_inform("Successfully deleted {sum(res == 200)} out of {length(obs_ids)} observations from ER")
}



get_sources <- function(api_base_url, token){
  
  api_endpnt <- file.path(api_base_url, "sources")
  
  req <- httr2::request(api_endpnt) |> 
    req_auth_bearer_token(token) |> 
    req_headers(
      "Accept" = "application/json",
      "Content-Type" = "application/json"
    )
  
  httr2::req_perform(req) |> 
    resp_body_json() |> 
    pluck("data") |> 
    pluck("results") 
}
  


delete_sources <- function(src_ids, api_base_url, token){
  
  #cli::cli_inform("Deleting sources")
  
  res <- map_dbl(src_ids, function(sid){
    #browser()
    #api_endpnt <- file.path(api_base_url, "source", sid)#, "?async=true")
    api_endpnt <- file.path(api_base_url, "source", sid, "?async")
    
    req <- request(api_endpnt) |>
      #req_url_query(async = TRUE) |>
      req_method("DELETE") |> 
      req_auth_bearer_token(token) |> 
      req_headers(
        "accept" = "application/json",
        "Content-Type" = "application/json"
      )
    #req_dry_run(req)
    
    req |> 
      httr2::req_retry(
        max_tries = 5,
        is_transient = \(resp) resp_status(resp) %in% c(429, 503, 504),
        backoff = \(resp) 10
      ) |>
      req_perform() |> 
      httr2::resp_status()
    
  }, .progress = TRUE)
  
  cli::cli_alert_info("Successfully deleted {sum(res == 200)} out of {length(src_ids)} source{?s} from ER.")
}




delete_subjects <- function(subj_ids, api_base_url, token){
  
  #cli::cli_inform("Deleting subjects")
  
  res <- map_dbl(subj_ids, function(id){
    #browser()
    req <- request(file.path(api_base_url, "subject", id)) |>
      req_method("DELETE") |> 
      req_auth_bearer_token(token) |> 
      req_headers(
        "accept" = "application/json",
        "Content-Type" = "application/json"
      )
    #req_dry_run(req)
    
    req_perform(req) |> httr2::resp_status()
  }, .progress = TRUE)
  
  cli::cli_alert_info("Successfully deleted {sum(res == 200)} out of {length(subj_ids)} subject{?s} from ER.")
}



# Erases all sources, related observations and assigned subjects associated to the
# specified source `provider`. Essentially performs a deep clean on ER's
# "Observations" section, allowing for clean testing
deep_clean_obs <- function(api_base_url, token, provider = "moveapps_ann_locs", sources_to_keep){
  
  cli::cli_inform("Cleaning Up sources, observations and subjects in ER")
  
  # get sources adnd their IDs
  obs_sources <- get_sources(api_base_url, token)
  
  source_ids <- purrr::keep(obs_sources, \(s) s$provider == provider) |> 
    purrr::map(~ data.frame(
      source_id = .x$id, 
      manufacturer_id = .x$manufacturer_id, 
      provider = .x$provider
    )) |> 
    list_rbind() |> 
    filter(manufacturer_id %notin% sources_to_keep)
  
  # get subjects assigned to sources
  source_subject <- purrr::map(
    source_ids$source_id, function(x){
      sub_dets <- get_source_subjects(x, api_base_url, token)[[1]]
      data.frame(
        source_id = x, 
        subject_id = sub_dets$id,
        subject_name = sub_dets$name
      )
    }) |> 
    purrr::list_rbind()
  
  # delete sources and, consequently, their respective observations entries
  delete_sources(source_ids$source_id, api_base_url, token)  
  
  # delete subjects assigned to sources, as they're retained in ER despite sources being deleted
  delete_subjects(subj_ids = source_subject$subject_id, api_base_url, token)
}






## /////////////////////////////////////////////////////////////////////////////
# helper to run SDK testing with different settings
run_sdk <- function(data, 
                    api_hostname = NULL,
                    api_token = NULL,
                    cluster_id_col = "clust_id",
                    lookback = 30L,
                    store_cols_str = NULL,
                    days_thresh = 7,
                    dist_thresh = 100,
                    match_criteria = "gmedian",
                    active_days_thresh = 14){

  require(jsonlite)
  
  # get environmental variables specified in .env
  dotenv::load_dot_env(".env")
  app_config_file <- Sys.getenv("CONFIGURATION_FILE")
  source_file <- Sys.getenv("SOURCE_FILE")

  # store default app configuration
  dflt_app_config <- jsonlite::fromJSON(app_config_file)
  # get default input data
  dflt_dt <- readRDS(source_file)

  # set configuration to specified inputs
  new_app_config <- list(
    api_hostname = api_hostname,
    api_token = api_token,
    cluster_id_col = cluster_id_col,
    lookback = lookback,
    store_cols_str = store_cols_str,
    days_thresh = days_thresh,
    dist_thresh = dist_thresh,
    match_criteria = match_criteria,
    active_days_thresh = active_days_thresh
  )
  
  # overwrite config file with current inputs
  write(
    jsonlite::toJSON(new_app_config, pretty = TRUE, auto_unbox = TRUE, null = "null"),
    file = app_config_file
  )

  # overwrite app's source file with current input data
  saveRDS(data, source_file)

  # run SDK for the current settings
  try(source("sdk.R"))

  # reset to default config and data
  write(
    jsonlite::toJSON(dflt_app_config,  pretty = TRUE, auto_unbox = TRUE),
    file = app_config_file
  )
  saveRDS(dflt_dt, source_file)

  invisible()
}



# ## /////////////////////////////////////////////////////////////////////////////
# # helper to run SDK testing with different settings
# run_sdk <- function(data, 
#                     cluster_id_col = "clust_id", 
#                     behav_col = "behav",
#                     cluster_tbl_type = c("track-and-whole")
# ){
# 
#   require(jsonlite)
# 
#   # get environmental variables specified in .env
#   dotenv::load_dot_env(".env")
#   app_config_file <- Sys.getenv("CONFIGURATION_FILE")
#   source_file <- Sys.getenv("SOURCE_FILE")
# 
#   # store default app configuration
#   dflt_app_config <- jsonlite::fromJSON(app_config_file)
#   # get default input data
#   dflt_dt <- readRDS(source_file)
# 
#   # set configuration to specified inputs
#   new_app_config <- list(
#     cluster_id_col = cluster_id_col,
#     behav_col = behav_col,
#     cluster_tbl_type = cluster_tbl_type
#   )
#   
#   # overwrite config file with current inputs
#   write(
#     jsonlite::toJSON(new_app_config, pretty = TRUE, auto_unbox = TRUE, null = "null"),
#     file = app_config_file
#   )
# 
#   # overwrite app's source file with current input data
#   saveRDS(data, source_file)
# 
#   # run SDK for the current settings
#   try(source("sdk.R"))
# 
#   # reset to default config and data
#   write(
#     jsonlite::toJSON(dflt_app_config,  pretty = TRUE, auto_unbox = TRUE),
#     file = app_config_file
#   )
#   saveRDS(dflt_dt, source_file)
# 
#   invisible()
# }


