library(rlang)
library(httr2)
library(lubridate)
library(move2)
#library(here)

if(rlang::is_interactive()){
  library(testthat)
  source("tests/app-testing-helpers.r")
  set_interactive_app_testing()
  app_key <- get_app_key()
  er_tokens <- httr2::secret_read_rds("dev/er_tokens.rds", key = I(app_key))
}


test_sets <- test_path("data/vult_unit_test_data.rds") |> 
  httr2::secret_read_rds(key = I(app_key)) 


# rFunction() --------------------------------------------------------------------------
test_that("output is a valid move2 object", {
  
  posting_dttm <- now()
  
  input_dt <- test_sets$nam_1 |> slice(1:10)
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature")
  
  output_dt <- rFunction(
    data = input_dt, 
    api_hostname = "standrews.dev.pamdas.org",
    api_token = er_tokens$standrews.dev$brunoc, 
    store_cols_str = paste(store_cols, collapse = ",")
  )
  
  # passes {move2} check
  expect_true(move2::mt_is_move2(ouput_dt))
  # check if 1st class is "move2"
  expect_true(class(ouput_dt)[1] == "move2")
  
  # input and output have the same nr of rows
  expect_equal(nrow(input_dt), nrow(output_dt))
  
  # specified store_cols are in output data
  expect_in(store_cols, names(output_dt))
  
  # attributes should be identical
  expect_identical(
    output_dt |> data.frame() |> select(timestamp, behav, geometry),
    input_dt |> data.frame() |> select(timestamp, behav, geometry)
  )
  
  # track ID column name has changed to `"track_id"`, but content remains the same
  expect_equal(output_dt$track_id, mt_track_id(input_dt))
  
  
  # delete test observations from ER
  ## first need to retrieve them to get the obs ids...
  pushed_test_obs <- get_obs(
    created_after = posting_dttm, 
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/", 
    token = er_tokens$standrews.dev$brunoc
  )
  # ... which can now be used to delete mentioned obs
  delete_obs(pushed_test_obs$id, er_tokens$standrews.dev$brunoc)
})





test_that("Input validation works as expected", {
  
  testthat::local_edition(3)
  
  # missing `hostname` or `token`
  expect_snapshot(rFunction(data = test_dt$nam_1), error = TRUE)
  expect_snapshot(rFunction(data = test_dt$nam_1, api_hostname = "bla.co.uk"), error = TRUE)
  
  
  # `cluster_id_col`: unspecified or absent from input data
  expect_snapshot(
    rFunction(
      data = test_dt$nam_1, 
      api_hostname = "bla.co.uk", 
      api_token = "XYZ", 
      cluster_id_col = NULL
    ), 
    error = TRUE
  )
  
  expect_snapshot(
    rFunction(
      data = test_dt$nam_1, 
      api_hostname = "bla.co.uk", 
      api_token = "XYZ", 
      cluster_id_col = "ABSENT_COLUMN"
    ), 
    error = TRUE
  )
  
  # `lookback`
  expect_snapshot(
    rFunction(
      data = test_dt$nam_1, 
      api_hostname = "bla.co.uk", 
      api_token = "XYZ", 
      cluster_id_col = "clust_id", 
      lookback = 1.2
    ), 
    error = TRUE
  )
  
  # # `store_cols_str`
  # rFunction(
  #   data = test_dt$nam_1, api_hostname = "bla.co.uk", api_token = "XYZ",
  #   store_cols_str = paste("NO_COLUMN_1", "NO_COLUMN_2", "NO_COLUMN_3", "clust_id", sep = ",")
  # )
  
})



test_that("dev testing", {
  
  skip()
  
  rFunction(
    #data = test_sets$nam_2 |> slice(30:70),
    data = test_sets$nam_2 |> slice(100:200), 
    #data = test_sets$nam_1 |> slice(40:80), 
    api_hostname = "standrews.dev.pamdas.org",
    api_token = er_tokens$standrews.dev$brunoc, 
    store_cols_str = paste(c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature"), collapse = ",")
  )
})




## get_hist()  -------------------------------------------------------------
test_that("get_hist() works as expected", {

  # setup   
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", 
                  "sunset_timestamp", "temperature")
  cluster_cols <- c("cluster_uuid", "cluster_status")
  
  dt <- test_sets$nam_1 |> 
    mutate(
      cluster_status = if_else(clust_id == "NAM.3", "CLOSED", "ACTIVE"),
      cluster_uuid = sub("NAM.", "CLST_", clust_id),
      track_id = move2::mt_track_id(test_sets$nam_1)
    ) |> 
    move2::mt_as_event_attribute(tag_id, individual_local_identifier, individual_id) |> 
    slice(1:50)
  
  posting_dttm <- now()
  
  # post data
  ra_post_obs(
    data = dt,
    tm_id_col = mt_time_column(dt),
    additional_cols = c(store_cols, cluster_cols),
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc#,
  )
  
  hist_dt <- fetch_hist(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    unclust_min_date = min(dt$timestamp) - lubridate::days(10), 
    page_size = 500
  )
  
  # expected to return all the pushed data
  expect_equal(nrow(hist_dt), nrow(dt))
  
  expect_identical(
    hist_dt |> 
      arrange(subject_id, recorded_at) |> 
      select(lat, lon, cluster_uuid,  behav),
    
    dt |> 
      arrange(individual_local_identifier, timestamp) |> 
      data.frame() |> 
      select(lat, lon, cluster_uuid, behav)
  )
  
  # delete test observations from ER
  delete_obs(hist_dt$er_obs_id, er_tokens$standrews.dev$brunoc)
  
})






## fetch_unclustered()  -------------------------------------------------------------
test_that("fill_track_gaps() works as expected", {
  
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", 
                  "sunset_timestamp", "temperature")
  
  cluster_cols <- c("cluster_uuid", "cluster_status")
  
  mv2_track_cols <- c("tag_id", "individual_local_identifier", "deployment_id", 
                      "individual_id", "track_id", "study_id")
  
  # set new data
  new <- slice(test_sets$nam_1, 50:120)
  
  # run cluster matching
  matched_hist <- match_sf_clusters(hist = NULL, new, "clust_id", "timestamp")
  
  # run merging
  merged_eg <- merge_and_update(
    matched_dt = matched_hist,
    new_dt = new, 
    cluster_id_col = "clust_id", 
    timestamp_col = "timestamp", 
    store_cols = store_cols
  )
  
  post_dttm <- now()
  
  merged_eg |> 
    dplyr::filter(request_type == "POST") |> 
    ra_post_obs(
      tm_id_col = "timestamp", 
      additional_cols =  c(store_cols, cluster_cols, mv2_track_cols), 
      api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/", 
      token = er_tokens$standrews.dev$brunoc, 
      provider_key = "moveapps_ann_locs", 
      batch_size = 200
    )
  
  merged_eg |> 
    dplyr::filter(request_type == "PATCH") |> 
    patch_obs(
      additional_cols = c(store_cols, cluster_cols, mv2_track_cols),
      api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/", 
      token = er_tokens$standrews.dev$brunoc
    )
  
  updated_clusters_uuid <- merged_eg |> 
    filter(!is.na(cluster_uuid), !is.na(request_type)) |> 
    distinct(cluster_uuid, request_type) |> 
    pull(cluster_uuid)
  
  clustered_eg <- merged_eg |> 
    filter(cluster_uuid %in% updated_clusters_uuid)
  
  out <- fill_track_gaps(
    clustered_dt = clustered_eg,
    tm_id_col = "timestamp",
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc
  )
  
  expect_equal(nrow(out), nrow(new))
  
  expect_equal(
    sum(is.na(out$cluster_uuid)),
    sum(is.na(new$clust_id))
  )
  
  # delete test observations from ER
  ## first need to retrieve them to get the obs ids...
  pushed_obs <- get_obs(
    created_after = post_dttm, 
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/", 
    token = er_tokens$standrews.dev$brunoc
  )
  # ... which can now be used to delete mentioned obs
  delete_obs(pushed_obs$id, er_tokens$standrews.dev$brunoc)
  
})








