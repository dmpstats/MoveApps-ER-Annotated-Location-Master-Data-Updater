source("tests/app-testing-helpers.r")
app_key <- get_app_key()
er_tokens <- httr2::secret_read_rds("dev/er_tokens.rds", key = I(app_key))

test_sets <- test_path("data/vult_unit_test_data.rds") |> 
  httr2::secret_read_rds(key = I(app_key)) 


dt <- test_sets$nam_1 |> 
  mutate(
    cluster_status = if_else(!is.na(clust_id), "active", NA),
    cluster_status = if_else(clust_id == "NAM.3", "closed", cluster_status)
  )


bs_url <- "https://standrews.dev.pamdas.org/api/v1.0/"
provkey <- "moveapps_ann_locs"
token <- er_tokens$standrews.dev$brunoc
lookback <- 10


# RA new obs   ------------------------------------------------------------

new_obs <-  dt[1:20, ] 

tm_id_col <- mt_time_column(new_obs)
trk_id_col <- mt_track_id_column(new_obs)


new_obs_jsonable <- new_obs |> 
  move2::mt_as_event_attribute(tag_id, individual_local_identifier, individual_id) |> 
  #sf::st_drop_geometry() |> 
  as.data.frame() |> 
  dplyr::mutate(
    # date-time cols formatted as ISO 8601 strings
    dplyr::across(dplyr::where(~inherits(.x, "POSIXt")), \(x) format(x, "%Y-%m-%d %X%z"))
  ) |> 
  mutate(
    recorded_at = .data[[tm_id_col]],
    manufacturer_id = as.character(tag_id),
    subject_name = as.character(individual_local_identifier),
    subject_type = "Wildlife",
    subject_subtype = "Unassigned", 
    source_type = "tracking_device",
    .keep = "unused"
  )  |> 
  tidyr::nest(
    location = c(lat, lon),
    .by = c(recorded_at:subject_subtype)
  ) |> 
  mutate(
    location = lapply(location, as.list)
  )


a <- paste0(bs_url, "sensors/dasradioagent/", provkey, "/status/")


req <- request(a) |>
  httr2::req_auth_bearer_token(token) |> 
  httr2::req_headers(
    "Accept" = "application/json",
    "Content-Type" = "application/json"
  ) |> 
  httr2::req_body_json(new_obs_jsonable)

req |> req_dry_run()

req |> httr2::req_error(~FALSE)|> 
  httr2::req_perform()

RA_time <- lubridate::now()



# -----------------------------------

# GET data just posted

b <- paste0(bs_url, "/observations/")

req <- httr2::request(b) |>
  httr2::req_url_query(
    created_after = format(RA_time - minutes(15), "%Y-%m-%d %X%z"),
    page_size = 500
  ) |>
  httr2::req_auth_bearer_token(token) |> 
  httr2::req_headers("accept" = "application/json")

req |> req_dry_run()

resp <- req |> httr2::req_error(~FALSE)|> 
  httr2::req_perform()

resp_dt <- resp_body_json(resp) |> 
  purrr::pluck("data") |>
  purrr::pluck("results") |> 
  lapply(data.frame) |> 
  purrr::list_rbind() |> 
  mutate(
    lat = location.latitude,
    lon = location.longitude,
    timestamp = lubridate::as_datetime(recorded_at),
    er_obs_id = id,
    .keep = "unused"
  )


# -------------------------------------------------------------------------------
# PATCH to append additional fields and cluster status (via exclusion flags)

dplyr::right_join(new_obs, resp_dt, by = c("timestamp", "lat", "lon")) |> 
  as.data.frame() |> 
  mutate(
    exclusion_flags = ifelse(cluster_status == "active", 8, 0),
    # exclusion_flags = dplyr::case_when(
    #   cluster_status == "active" ~ 8,
    #   cluster_status == "closed" ~ 4,
    #   is.na(cluster_status) ~ 0
    # ),
    #drop units
    dplyr::across(dplyr::where(~inherits(.x, "units")), .fns = \(x) units::set_units(x, NULL))
  ) |> 
  dplyr::group_split(er_obs_id) |> 
  purrr::map(function(obs){
    
    #browser()
    
    api <- file.path(bs_url, "observation", obs$er_obs_id)
    
    req <- httr2::request(api) |> 
      httr2::req_auth_bearer_token(token) |> 
      req_method("PATCH") |> 
      httr2::req_headers(
        "Accept" = "application/json",
        "Content-Type" = "application/json"
      ) |> 
      httr2::req_body_json(
        list(
          exclusion_flags = obs$exclusion_flags,
          additional = as.list(obs[c("clust_id", "cluster_status", "behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature")])
        )
      )
    
    #req |> req_dry_run()
    
    res <- req |> httr2::req_error(~FALSE)|> 
      httr2::req_perform()
    
    res |> resp_status()
    
  }, 
  .progress = TRUE
  )





# -------------------------------------------------------------------------------
# GET current historical data

b <- paste0(bs_url, "/observations/")

req_hist <- httr2::request(b) |>
  httr2::req_url_query(
    since = format(min(dt[[tm_id_col]]) - lubridate::days(lookback), "%Y-%m-%d %X%z"),
    filter = 8,
    page_size = 500,
    include_details = tolower(as.character(TRUE))
  ) |>
  httr2::req_auth_bearer_token(token) |> 
  httr2::req_headers("accept" = "application/json")

req_hist |> req_dry_run()

resp_hist <- req_hist |> httr2::req_error(~FALSE)|> 
  httr2::req_perform()

hist_pre_dt <- resp_body_json(resp_hist) |> 
  purrr::pluck("data") |>
  purrr::pluck("results") |> 
  lapply(data.frame) |> 
  purrr::list_rbind() |> 
  mutate(
    lat = location.latitude,
    lon = location.longitude,
    timestamp = lubridate::as_datetime(recorded_at),
    er_obs_id = id,
    .keep = "unused"
  )



unique(hist_dt$source) |> 
  map(function(src){
    
    req_url <- file.path(bs_url, "source", src)
    
    req_src <- httr2::request(req_url) |>
      httr2::req_auth_bearer_token(token) |> 
      httr2::req_headers("accept" = "application/json")
    
    req_src |> req_dry_run()
    
    res_src <- req_src |> 
      #httr2::req_error(~FALSE)|> 
      httr2::req_perform()
    
    src_dt <- resp_body_json(res_src) |> 
      purrr::pluck("data")
    
    src_dt$manufacturer_id
    
  })




get_source_details <- function(src, bs_url, token){
  
  #browser()
  
  req_url <- file.path(bs_url, "source", src)
  
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



get_source_subjects <- function(src, bs_url, token){
  
  req_url <- file.path(bs_url, "source", src, "subjects")
  
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


test <- get_source_subjects(hist_pre_dt$source[1], bs_url, token)
map_chr(test, ~.x$name)





hist_dt <- hist_pre_dt |> 
  group_by(source) |> 
  mutate(
    tag_id = get_source_details(unique(source), bs_url, token)$manufacturer_id,
    subject_name = get_source_subjects(unique(source), bs_url, token)[[1]]$name
  )


hist_dt
new_obs |> as.data.frame()






