#library(move2)
#library(withr)
#library(dplyr)
library(rlang)
library(httr2)
library(lubridate)
#library(sf)
#library(here)

if(rlang::is_interactive()){
  library(testthat)
  source("tests/app-testing-helpers.r")
  set_interactive_app_testing()
  app_key <- get_app_key()
}


test_sets <- test_path("data/vult_unit_test_data.rds") |> 
  httr2::secret_read_rds(key = I(app_key)) 

er_tokens <- httr2::secret_read_rds("dev/er_tokens.rds", key = I(app_key))


# er_check_status() ----------------------------------------------

test_that("get_obs() dev testing", {
  
  skip()
  
  # get obs annotated as members of "active" clusters
  get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    filter = 8,
    min_date = as.POSIXct("2000-01-01") - lubridate::days(50),
    max_date = NULL, 
    page_size = 100, 
    include_details = TRUE,
    created_after = NULL
  )
  
  # get all obs, regardless of cluster membership and cluster-status
  get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    filter = 'null',
    min_date = as.POSIXct("2000-01-01") - lubridate::days(50),
    max_date = NULL, 
    page_size = 100, 
    include_details = TRUE,
    created_after = NULL
  )
  
})




test_that("ra_post_obs() dev testing", {
  
  skip()
  
  tm_id_col <- mt_time_column(test_sets$nam_1)
  trk_id_col <- mt_track_id_column(test_sets$nam_1)
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature")
  
  dt <- test_sets$nam_1 |> 
    mutate(
      cluster_status = if_else(!is.na(clust_id), "active", NA),
      cluster_status = if_else(clust_id == "NAM.3", "closed", cluster_status)
    ) |> 
    move2::mt_as_event_attribute(tag_id, individual_local_identifier, individual_id) |> 
    as.data.frame() |> 
    slice(21:150)
    
  ra_post_obs(
    data = dt,
    tm_id_col = tm_id_col,
    trk_id_col = trk_id_col, 
    store_cols = store_cols,
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc,
    batch_size = 10
  )
  
})





test_that("send_new_obs() dev testing", {
  
  skip()
  
  tm_id_col <- mt_time_column(test_sets$nam_1)
  trk_id_col <- mt_track_id_column(test_sets$nam_1)
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature")
  
  dt <- test_sets$nam_1 |> 
    mutate(
      cluster_status = if_else(!is.na(clust_id), "active", NA),
      cluster_status = if_else(clust_id == "NAM.3", "closed", cluster_status)
    ) |> 
    move2::mt_as_event_attribute(tag_id, individual_local_identifier, individual_id) |> 
    as.data.frame() |> 
    slice(21:25)
  
  
  send_new_obs(
    new_dt = dt, 
    tm_id_col = tm_id_col, 
    trk_id_col = trk_id_col, 
    store_cols = store_cols,
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/", 
    token = er_tokens$standrews.dev$brunoc
  )
  
  
})





test_that("fetch_hist() dev testing", {
  
  skip()
  
  fetch_hist(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc
  )
  
  
})







test_that("er_get_obs() dev testing", {
  
  skip()
  
  er_get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    filter = 8,
    min_date = as.POSIXct("2000-01-01") - lubridate::days(50),
    max_date = NULL, 
    page_size = 100, 
    include_details = TRUE,
    created_after = NULL
  )
  
})







test_that("er_check_status() dev testing", {
  
  skip()
  
  er_check_status(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = "ttetet" #er_tokens$standrews.dev$brunoc
  )
  
})






test_that("send_new_obs() dev testing", {
  
  skip() 
  
  dt <- test_sets$nam_1 |> 
    mutate(
      cluster_status = if_else(!is.na(clust_id), "active", NA),
      cluster_status = if_else(clust_id == "NAM.3", "closed", cluster_status)
    )
  
  post_new_obs(
    data = dt[1:20, ], 
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/", 
    token = er_tokens$standrews.dev$brunoc, 
    store_cols = c("clust_id", "behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature")
  )
})
