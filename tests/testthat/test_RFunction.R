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
  
  posting_dttm <- now() - seconds(30)
  
  input_dt <- test_sets$nam_1 |> slice(1:10)
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature")
  
  output_dt <- rFunction(
    data = input_dt, 
    api_hostname = "standrews.dev.pamdas.org",
    api_token = er_tokens$standrews.dev$brunoc, 
    store_cols_str = paste(store_cols, collapse = ",")
  )
  
  # passes {move2} check
  expect_true(move2::mt_is_move2(output_dt))
  # check if 1st class is "move2"
  expect_true(class(output_dt)[1] == "move2")
  
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
  expect_snapshot(rFunction(data = test_sets$nam_1), error = TRUE)
  expect_snapshot(rFunction(data = test_sets$nam_1, api_hostname = "bla.co.uk"), error = TRUE)
  
  
  # `cluster_id_col`: unspecified or absent from input data
  expect_snapshot(
    rFunction(
      data = test_sets$nam_1, 
      api_hostname = "bla.co.uk", 
      api_token = "XYZ", 
      cluster_id_col = NULL
    ), 
    error = TRUE
  )
  
  expect_snapshot(
    rFunction(
      data = test_sets$nam_1, 
      api_hostname = "bla.co.uk", 
      api_token = "XYZ", 
      cluster_id_col = "ABSENT_COLUMN"
    ), 
    error = TRUE
  )
  
  # `lookback`
  expect_snapshot(
    rFunction(
      data = test_sets$nam_1, 
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




test_that("Fused clusters are signalled in output", {
  
  deep_clean_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
  )
  
  
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature")
  cluster_cols <- c("cluster_uuid", "cluster_status")
  
  dt <- test_sets$nam_1 |> 
    filter(clust_id %in% c("NAM.3"))
  
  # define historic data and upload it to ER
  hist <- dt |> 
    mutate(
      clust_id = case_when(
        clust_id == "NAM.3" & row_number() <= 20 ~ "NAM.000",
        clust_id == "NAM.3" & row_number() > 20 ~ "NAM.001",
        is.na(clust_id) ~ NA
      ),
      cluster_uuid = sub("NAM.", "CLST_", clust_id),
      cluster_status = ifelse(!is.na(cluster_uuid), "ACTIVE", NA),
      track_id = move2::mt_track_id(dt)
    ) |> 
    move2::mt_as_event_attribute(tag_id, deployment_id, individual_local_identifier, individual_id)
  
  ra_post_obs(
    data = hist,
    tm_id_col = mt_time_column(hist),
    additional_cols = c(store_cols, cluster_cols),
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc
  )
  
  
  # define current data and run rFunction
  # set new data (containing the fusing event)
  new <- mutate(dt, clust_id = "FUSION_CASE_1")
  
  output_dt <- rFunction(
    data = new, 
    api_hostname = "standrews.dev.pamdas.org",
    api_token = er_tokens$standrews.dev$brunoc, 
    store_cols_str = paste(store_cols, collapse = ",")
  )
  
  
  # check if row with fused cluster exists and value is as expected
  expect_equal(
    output_dt |> 
      filter(track_id == "DISPERSED_CLUSTERS_TRACKER") |> 
      pull(cluster_uuid) |> 
      unique(),
    "CLST_001"
  )
  
  # output should have 1 more row that new data, which contains uuid of fused cluster
  expect_true( nrow(output_dt) == nrow(new) + 2)
  
  deep_clean_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc,
    sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID") 
  )
  
})



test_that("Inclusion of 'lat'/'lon' in `store_cols_str` is handled appropriately", {
  
  posting_dttm <- now() - seconds(30)
  
  ## "lat" & "lon" included in `store_cols_str` ------
  expect_no_error(
    out <- rFunction(
      data = test_sets$nam_2 |> slice(100:150), 
      api_hostname = "standrews.dev.pamdas.org",
      api_token = er_tokens$standrews.dev$brunoc, 
      store_cols_str = paste(c("lat", "lon", "behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature"), collapse = ",")
    )
  )
  
  ### lat-lon in output
  expect_true(all(c("lat", "lon") %in% names(out)))
  
  ### fetch uploaded data
  pushed_obs <- get_obs(
    created_after = posting_dttm, 
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/", 
    token = er_tokens$standrews.dev$brunoc
  )
  
  ### lat-lon in master data stored in EarthRanger
  expect_true(all(c("lat", "lon") %in% names(pushed_obs)))
  
  # clean pushed obs
  delete_obs(pushed_obs$id, er_tokens$standrews.dev$brunoc)
  
  
  ## Differently named lat/lon cols are stored and returned ----
  out <- rFunction(
    data = test_sets$nam_2 |> slice(100:150) |> rename(latitude = lat, Longitude = lon), 
    api_hostname = "standrews.dev.pamdas.org",
    api_token = er_tokens$standrews.dev$brunoc, 
    store_cols_str = paste(c("latitude", "Longitude", "behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature"), collapse = ",")
  )
  
  ### "latitude" and "Longitude" in output
  expect_true(all(c("latitude", "Longitude") %in% names(out)))
  
  ### fetch uploaded data
  pushed_obs <- get_obs(
    created_after = posting_dttm, 
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/", 
    token = er_tokens$standrews.dev$brunoc
  )
  
  ### "lat" & "lon" in stored master data...
  expect_true(all(c("lat", "lon") %in% names(pushed_obs)))
  
  ### .. as well as "latitude" and "Longitude" (duplicating the information)
  expect_true(all(c("latitude", "Longitude") %in% names(pushed_obs)))
  
  # clean pushed obs
  delete_obs(pushed_obs$id, er_tokens$standrews.dev$brunoc)
  
  
  ## Non-selecting lat/lon cols leaves them out  ----
  out <- rFunction(
    data = test_sets$nam_2 |> slice(100:150), 
    api_hostname = "standrews.dev.pamdas.org",
    api_token = er_tokens$standrews.dev$brunoc, 
    store_cols_str = paste(c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature"), collapse = ",")
  )
  
  ### "lat" & "lon" not in in output, despite being in input 
  expect_true(!all(c("lat", "lon") %in% names(out)))
  
  ### fetch uploaded data
  pushed_obs <- get_obs(
    created_after = posting_dttm, 
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/", 
    token = er_tokens$standrews.dev$brunoc
  )
  
  ### "lat" & "lon" always stored master data as point coords
  expect_true(all(c("lat", "lon") %in% names(pushed_obs)))
  
  deep_clean_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
  )
  
})





test_that("Absence of 'lat'/'lon' cols in input data handled as expected", {
  
  expect_no_error(
    out <- rFunction(
      data = test_sets$nam_2 |> slice(100:120) |> select(-c(lat, lon)), 
      api_hostname = "standrews.dev.pamdas.org",
      api_token = er_tokens$standrews.dev$brunoc, 
      store_cols_str = paste(c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature"), collapse = ",")
    )
  )
  
  deep_clean_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
  )
  
})




test_that("Presence of NAs in lat/lon cols of input data handled as expected", {
  
  # If not handled adequately, NAs in lat-lon cols will cause error in
  # radio-agent posting. Code checks presence of NAs, recalculating lat-lon
  # columns accordingly
  expect_no_error(
    out <- rFunction(
      data = test_sets$nam_2 |> slice(1:10) |> 
        mutate(
          lat = if_else(dplyr::row_number() %in% c(2, 4), NA, lat),
          lon = if_else(dplyr::row_number() %in% c(2, 4), NA, lon)
        ), 
      api_hostname = "standrews.dev.pamdas.org",
      api_token = er_tokens$standrews.dev$brunoc, 
      store_cols_str = paste(c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature"), collapse = ",")
    )
  )
  
  deep_clean_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
  )
  
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
  
  
  rFunction(
    data = test_sets$nam_2 |> slice(100:200), 
    api_hostname = "standrews.dev.pamdas.org",
    api_token = er_tokens$standrews.dev$brunoc, 
    store_cols_str = paste(c("lat", "lon", "behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature"), collapse = ",")
  )
  
  deep_clean_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
  )
  
  
  
  test <- read_rds("c:/Users/Bruno/Downloads/Test_MA_ER_Master_Updater_App__Namibia_Study__Avian_Cluster_Detection__2025-07-25_17-02-05.rds")
  
  rFunction(
    data = test, 
    #cluster_id_col = "clust_id",
    api_hostname = "standrews.dev.pamdas.org",
    api_token = er_tokens$standrews.dev$brunoc, 
    store_cols_str = paste(c("lat", "lon", "behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature"), collapse = ",")
  )
  
  
  deep_clean_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc,
    sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
  )
    
    
    
    
  
})



## coerce_col_types() -----------------------------------------------------

test_that(" coerce_col_types() works as expected", {
  
  dt <- tibble::tibble(
    a = 1:5,
    b = letters[1:5],
    c = bit64::as.integer64(6:10),
    d = units::set_units(11:15, "m/s"),
    e = seq.POSIXt(as.POSIXct("2025-05-01 12:03:01"), as.POSIXct("2025-05-05 09:03:01"), length.out = 5),
    g = units::set_units(21:25, "degrees"),
    h = units::set_units(51:55, "m")
  )
  
  dt_2 <- tibble::tibble(
    b = letters[1:2],
    c =  11:12,
    d = 21:22,
    e = c("2021-10-01 12:03:01", "2021-10-01 12:10:01"),
    f = factor(c(8, 9)),
    # test handling of NA
    g = NA_character_,
    h = units::set_units(2:3, "km")
  )
  
  out <- coerce_col_types(dt_2, dt)
  
  ### Covered columns ------
  # common cols between reference and target datasets have identical classes
  expect_equal(class(dt$b), class(out$b))
  expect_equal(class(dt$c), class(out$c))
  expect_equal(class(dt$d), class(out$d))
  expect_equal(class(dt$e), class(out$e))
  
  ### units columns ------
  #### numeric target coerced to same units as reference
  expect_equal(units::deparse_unit(dt$d), units::deparse_unit(out$d))
  #### NA_character_ target handled accordingly
  expect_equal(units::deparse_unit(dt$g), units::deparse_unit(out$g))
  #### units in target converted to units in reference
  expect_equal(units::deparse_unit(dt$h), units::deparse_unit(out$h))
  
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
    move2::mt_as_event_attribute(tag_id, deployment_id, individual_local_identifier, individual_id) |> 
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
      arrange(er_subject_id, recorded_at) |> 
      select(lat, lon, cluster_uuid,  behav),
    
    dt |> 
      arrange(individual_local_identifier, timestamp) |> 
      data.frame() |> 
      select(lat, lon, cluster_uuid, behav)
  )
  
  # delete test observations from ER
  delete_obs(hist_dt$er_obs_id, er_tokens$standrews.dev$brunoc)
  
})






## fill_track_gaps()  -------------------------------------------------------------
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




# is_masked_bool() --------------

test_that("is_masked_bool(): TRUE when all non-NA character elements are whole-word 'true' or 'false' (any case)", {
  expect_true(is_masked_bool(c("true", "FALSE")))
  # NA ignored, remaining matches -> TRUE
  expect_true(is_masked_bool(c("TRUE", "True", NA_character_)))
  expect_true(is_masked_bool(c("false", "true", "FALSE")))
  expect_true(is_masked_bool(c("False", "true", "")))
})


test_that("is_masked_bool(): FALSE when any non-NA element is not a whole-word match", {
  expect_false(is_masked_bool(c("true", "no")))
  # substring is not a whole-word, so first element fails
  expect_false(is_masked_bool(c("someFalseValue", "false")))  
  expect_false(is_masked_bool(c("nottruehere", "falsehood")))
})


test_that("is_masked_bool(): FALSE for non-character inputs", {
  # logical
  expect_false(is_masked_bool(c(TRUE, FALSE)))
  # factor
  expect_false(is_masked_bool(factor("true")))
  # numeric
  expect_false(is_masked_bool(c(1, 0)))
})


test_that("is_masked_bool(): FALSE for empty or all-NA character vectors", {
  # empty character
  expect_false(is_masked_bool(character(0)))
  # all NAs
  expect_false(is_masked_bool(c(NA_character_, NA_character_)))
  # empty strings
  expect_false(is_masked_bool(c("", "")))
  expect_false(is_masked_bool(c(NA_character_, "")))
})


test_that("is_masked_bool(): FALSE for mixed NA and non-matching values", {
  expect_false(is_masked_bool(c(NA_character_, "no", NA_character_))) # non-matching present -> FALSE
})



# bind_latlon() ---------------------------------------------

test_that("bind_latlon() correctly attaches lon/lat for longlat sf object", {
  coords <- data.frame(X = c(10, 20), Y = c(-5, 15))
  pts <- st_as_sf(coords, coords = c("X", "Y"), crs = 4326)
  pts_new <- bind_latlon(pts)
  expect_true(all.equal(pts_new$lon, coords$X))
  expect_true(all.equal(pts_new$lat, coords$Y))
})


test_that("bind_latlon() ataches lon/lat but original projection is kept", {
  coords <- data.frame(X = c(500000, 400000), Y = c(4649776, 4650000))
  pts <- st_as_sf(coords, coords = c("X", "Y"), crs = 32633)
  pts_new <- bind_latlon(pts)
  
  expect_contains(names(pts_new), c("lat", "lon"))
  expect_equal(sf::st_crs(pts_new), sf::st_crs(pts))
  
  # Coordinates must be numeric lat/lon
  expect_type(pts_new$lon, "double")
  expect_type(pts_new$lat, "double")
  expect_equal(length(pts_new$lon), 2)
})


# Overwrites existing lon/lat columns
test_that("bind_latlon() overwrites existing lon/lat columns", {
  coords <- data.frame(X = 0, Y = 0, lon = NA, lat = NA)
  pts <- st_as_sf(coords, coords = c("X", "Y"), crs = 4326)
  pts_new <- bind_latlon(pts)
  expect_false(any(is.na(pts_new$lon)))
  expect_false(any(is.na(pts_new$lat)))
})

