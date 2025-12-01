#library(move2)
#library(withr)
library(dplyr)
library(rlang)
library(httr2)
library(lubridate)
library(lubridate)
#library(sf)
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

# nam_lrg <- httr2::secret_read_rds("data/raw/vult_test_data_nam3mths.rds", key = I(app_key)) |> 
#   slice(1:8000)

#active_flag <- bit64::as.integer64(1311673391471656960)


# ra_post_obs() & get_obs() ---------------------------------------------

test_that("ra_post_obs() fails if key required columns are missing", {
  
  testthat::local_edition(3)
  
  expect_snapshot(
    ra_post_obs(
      data = test_sets$nam_1 |> slice(1:5),
      tm_id_col = mt_time_column(test_sets$nam_1),
      #additional_cols = store_cols,
      api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
      token = er_tokens$standrews.dev$brunoc
    ), 
    error = TRUE)
})



test_that("get_obs(): fails when and as expected", {
  
  testthat::local_edition(3)
  
  # invalid url
  expect_snapshot(
    get_obs(
      api_base_url = "https://WRONG_URL.co.uk",
      token = er_tokens$standrews.dev$brunoc
    ), 
    error = TRUE
  )
  
  # invalid token
  expect_snapshot(
    get_obs(
      api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
      token = "INVALID_TOKEN"
    ), 
    error = TRUE
  )
  
  # invalid date arguments
  expect_snapshot(get_obs(min_date = "INVALID DATE" ), error = TRUE)
  expect_snapshot(get_obs(max_date = "INVALID DATE" ), error = TRUE)
  expect_snapshot(get_obs(created_after = "INVALID DATE" ), error = TRUE)
  
  # invalid `filter`
  expect_snapshot(get_obs(filter = "INVALID_FILTER"), error = TRUE)
  expect_snapshot(get_obs(filter = factor(2)), error = TRUE)
})



test_that("Data posted and retrieved as expected: basic case", {
  
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
    slice(1:5)
  
  posting_dttm <- now() - seconds(5)
  
  expect_no_error(
    ra_post_obs(
    data = dt,
    tm_id_col = mt_time_column(dt),
    additional_cols = c(store_cols, cluster_cols),
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc
    )
  )
  
  # get all obs created after the above post request
  dt_retrieved <- get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    include_details = TRUE, 
    created_after = posting_dttm
  )
  
  # check dimensions
  expect_equal(nrow(dt_retrieved), nrow(dt))
  
  # check values
  expect_equal(dt_retrieved$lat, dt$lat)
  expect_equal(dt_retrieved$cluster_status, dt$cluster_status)
  
  # check additional columns are posted and retrieved
  expect_contains(names(dt_retrieved), store_cols)
  expect_contains(names(dt_retrieved), cluster_cols)
  
  # delete test observations from ER
  delete_obs(dt_retrieved$id, er_tokens$standrews.dev$brunoc)
  
})




test_that("Data posted and retrieved as expected: handling of obs in ACTIVE clusters", {
  
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", 
                  "sunset_timestamp", "temperature")
  cluster_cols <- c("cluster_uuid", "cluster_status")
  
  # test set with the 3 cases for cluster_status
  dt <- test_sets$nam_1 |> 
    filter(clust_id %in% c("NAM.1", "NAM.2", "NAM.3")) |> 
    mutate(
      cluster_status = case_when(
        clust_id == "NAM.2" ~ "CLOSED", 
        clust_id == "NAM.1" ~ "ACTIVE"
      ),
      cluster_uuid = sub("NAM.", "CLST_", clust_id),
      track_id = .data[[move2::mt_track_id_column(test_sets$nam_1)]]
    ) |> 
    move2::mt_as_event_attribute(tag_id, deployment_id, individual_local_identifier, individual_id) |> 
    slice_sample(n = 5, by = clust_id) |> 
    arrange(individual_local_identifier, timestamp)
  
  posting_dttm <- now() - seconds(5)
  
  expect_no_error(
    ra_post_obs(
      data = dt,
      tm_id_col = mt_time_column(dt),
      additional_cols = c(store_cols, cluster_cols),
      api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
      token = er_tokens$standrews.dev$brunoc#,
    )
  )
  
  # get obs annotated as members of ACTIVE clusters
  dt_retrieved_actv <- get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    include_details = TRUE, 
    filter = active_flag,
    created_after = posting_dttm
  )
  
  # check dimensions
  dt_active <- filter(dt, cluster_status == "ACTIVE")
  expect_equal(nrow(dt_retrieved_actv), nrow(dt_active))
  
  # check values
  expect_equal(dt_retrieved_actv$lat, dt_active$lat)
  expect_equal(dt_retrieved_actv$cluster_status, dt_active$cluster_status)
  expect_equal(ymd_hms(dt_retrieved_actv$recorded_at), dt_active$timestamp)
  
  
  # get obs annotated as members of CLOSED clusters AND non-clustered
  dt_retrieved_non_actv <- get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    include_details = TRUE, 
    filter = 0,
    created_after = posting_dttm
  ) |> 
    dplyr::filter(exclusion_flags == 0)
  
  dt_closed <- filter(dt, cluster_status == "CLOSED")
  dt_unclust <- filter(dt, is.na(cluster_status))
  dt_retrieved_closed <- filter(dt_retrieved_non_actv, cluster_status == "CLOSED")
  dt_retrieved_unclust <- filter(dt_retrieved_non_actv, is.na(cluster_status))
  
  # check dimensions
  expect_equal(nrow(dt_retrieved_closed), nrow(dt_closed))
  expect_equal(nrow(dt_retrieved_unclust), nrow(dt_unclust))
  
  # check values
  expect_equal(dt_retrieved_closed$lat, dt_closed$lat)
  expect_equal(dt_retrieved_unclust$cluster_status, dt_unclust$cluster_status)
  expect_equal(dt_retrieved_closed$cluster_status, dt_closed$cluster_status)
  expect_equal(ymd_hms(dt_retrieved_closed$recorded_at), dt_closed$timestamp)
  
  # delete test observations from ER
  delete_obs(c(dt_retrieved_actv$id, dt_retrieved_non_actv$id), token = er_tokens$standrews.dev$brunoc)
  
})




test_that("Data posted and retrieved as expected: fetch obs by subject ID", {
  
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
    slice(1:100)
  
  posting_dttm <- now() - seconds(10)
  
  expect_no_error(
    ra_post_obs(
      data = dt,
      tm_id_col = mt_time_column(dt),
      additional_cols = c(store_cols, cluster_cols),
      api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
      token = er_tokens$standrews.dev$brunoc#,
    )
  )
  
  subjs <- get_subject_ids("https://standrews.dev.pamdas.org/api/v1.0/", er_tokens$standrews.dev$brunoc)
  
  # get obs of subject named "GA_6581"
  dt_GA_6581 <- get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    include_details = TRUE, 
    subject_id = subjs[subjs$er_subject_name == "GA_6581", "er_subject_id"]
  )
  
  expect_equal(
    nrow(dt_GA_6581),
    nrow(filter(dt, individual_local_identifier ==  "GA_6581"))
  )
  
  expect_identical(
    dt_GA_6581 |> select(lat, lon, cluster_uuid),
    dt |> data.frame() |> filter(individual_local_identifier ==  "GA_6581") |> select(lat, lon, cluster_uuid )
  )
  
  # Delete test observations from ER
  # get All obs, for cleaning
  ER_obs <- get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    created_after = posting_dttm
  )
  # delete
  delete_obs(ER_obs$id, token = er_tokens$standrews.dev$brunoc)
  
})





# patch_obs()  -------------------------------------------------------------

test_that("Data patched as expected", {
  
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", 
                  "sunset_timestamp", "temperature")
  cluster_cols <- c("cluster_uuid", "cluster_status")
  
  dt <- test_sets$nam_1 |> 
    mutate(
      cluster_status = NA_character_,
      cluster_uuid = NA_character_,
      track_id = move2::mt_track_id(test_sets$nam_1)
    ) |> 
    move2::mt_as_event_attribute(tag_id, deployment_id, individual_local_identifier, individual_id) |> 
    slice(1:20)
  
  # store posting date-time for later reference for GET requests based on "created_after"
  posting_dttm <- now() - seconds(10)
  
  # post data
  ra_post_obs(
    data = dt,
    tm_id_col = mt_time_column(dt),
    additional_cols = c(store_cols, cluster_cols),
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc#,
  )
  
  # get data
  dt_retrieved <- get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    created_after = posting_dttm
  ) |> 
    select(id, recorded_at, lat, lon) |> 
    mutate(
      er_obs_id = id,
      timestamp = ymd_hms(recorded_at), 
      .keep = "unused"
    )
  
  # combine data, to link observation IDS
  dt <- left_join(dt, dt_retrieved, by = c("timestamp", "lat", "lon"))
  
  ## testing change in observation's membership -----------------
  obs_changed <- slice(dt, 1:2) |> 
    mutate(cluster_status = "ACTIVE", cluster_uuid = "THIS_IS_A_CLUSTER_UUID")
  
  patch_obs(
    obs_changed, 
    additional_cols = c(store_cols, cluster_cols),
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc
  )
    
  dt_patched_retrieved_actv <- get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    filter = active_flag,
    created_after = posting_dttm
  )
    
  expect_equal(dt_patched_retrieved_actv$id, obs_changed$er_obs_id)
  expect_equal(dt_patched_retrieved_actv$cluster_uuid, obs_changed$cluster_uuid)
  expect_equal(dt_patched_retrieved_actv$cluster_status, obs_changed$cluster_status)
  
  
  ## testing change in one of the `store_cols` data  -----------------
  obs_changed <- slice(dt, 3:5) |> mutate(behav = "SNOOZING")
  
  patch_obs(
    obs_changed, 
    additional_cols = c(store_cols, cluster_cols),
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc
  )
  
  dt_patched_retrieved <- get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    created_after = posting_dttm
  )
  
  expect_equal(sum(dt_patched_retrieved$behav == "SNOOZING"), nrow(obs_changed))
  
  # delete test observations from ER
  delete_obs(dt_retrieved$er_obs_id, er_tokens$standrews.dev$brunoc)
})






# Development Testing  -------------------------------------------------------------
test_that("get_obs() dev testing", {
  
  skip()
  
  
  # get obs annotated as members of "active" clusters
  test <- get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    filter = active_flag,
    min_date = lubridate::now() - lubridate::days(40),
    max_date = NULL, 
    page_size = 200, 
    include_details = TRUE,
    created_after = NULL
  )
  
  nrow(test)
  
  test <- get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    filter = active_flag,
    min_date = lubridate::now() - lubridate::days(2),
    max_date = NULL, 
    page_size = 100, 
    include_details = TRUE,
    created_after = NULL
  )
  
  
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
    #filter = 'null',
    min_date = as.POSIXct("2000-01-01") - lubridate::days(50),
    max_date = NULL, 
    page_size = 100, 
    include_details = TRUE,
    created_after = NULL
  )
  
  
  
  test <- get_obs(
    api_base_url = "https://ncz-vultures-test.pamdas.org/api/v1.0",
    token = er_tokens$`ncz-vultures-test`$brunoc,
    filter = 0,
    min_date = lubridate::now() - lubridate::years(2),
    max_date = NULL, 
    page_size = 100, 
    include_details = TRUE,
    created_after = NULL
  )
  
  
  
  test <- get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0",
    token = er_tokens$standrews.dev$brunoc,
    filter = 0,
    min_date = lubridate::now() - lubridate::days(5),
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
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature", "cluster_status")
  
  dt <- test_sets$nam_1 |> 
    mutate(
      cluster_status = if_else(!is.na(clust_id), "ACTIVE", NA),
      cluster_status = if_else(clust_id == "NAM.3", "CLOSED", cluster_status),
      cluster_uuid = sub("NAM.", "CLST_", clust_id),
      track_id = move2::mt_track_id(test_sets$nam_1)
    ) |> 
    move2::mt_as_event_attribute(tag_id, deployment_id, individual_local_identifier, individual_id) |> 
    as.data.frame() |> 
    slice(1:50)
  
  additional_cols <- c(store_cols, "cluster_uuid", "cluster_status", "track_id", "individual_id", "deployment_id", "study_id")
  
  ra_post_obs(
    data = dt,
    tm_id_col = tm_id_col,
    additional_cols = additional_cols,
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc#,
    #batch_size = 20
  )
  
  
  
  dt |> 
    slice(1) |> 
    mutate(
      individual_local_identifier = "Gervasio",
      tag_id = bit64::as.integer64(123456)
    ) |> 
    ra_post_obs(
      tm_id_col = tm_id_col,
      additional_cols = additional_cols,
      api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
      token = er_tokens$standrews.dev$brunoc#,
      #batch_size = 20
    )
  
  
  dt |> 
    slice(2) |> 
    mutate(
      individual_local_identifier = "Belchior",
      tag_id = bit64::as.integer64(123456)
    ) |> 
    ra_post_obs(
      tm_id_col = tm_id_col,
      additional_cols = additional_cols,
      api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
      token = er_tokens$standrews.dev$brunoc
    )
  
  
  
  
  dt |> 
    slice(3) |> 
    mutate(
      individual_local_identifier = "Gervasio",
      tag_id = bit64::as.integer64(1098765)
    ) |> 
    ra_post_obs(
      tm_id_col = tm_id_col,
      additional_cols = additional_cols,
      api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
      token = er_tokens$standrews.dev$brunoc
    )
  
  
  bit64::abs.integer64(c(1080727521 + 838981044))
  
  
  dt |> 
    slice(1:2) |> 
    mutate(
      individual_local_identifier = c("Belchior", "Gervasio"),
      tag_id = bit64::as.integer64(123456)
    ) |> 
    ra_post_obs(
      tm_id_col = tm_id_col,
      additional_cols = additional_cols,
      api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
      token = er_tokens$standrews.dev$brunoc#,
      #batch_size = 20
    )
  
  
  
})



test_that("get_source_details() dev testing", {
  
  skip()
  
  test <- get_source_details(
    src = "838981044",
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token =er_tokens$standrews.dev$brunoc
  )
  
  ?reframe
  
  tibble::as_tibble(test, )
  
})


test_that("patch_obs() dev testing", {
  
  skip()
  
  #### PATCHing retried if API returns 502 error -----
  #### currently occurring when number of  sequential requests is very high
  
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", 
                  "sunset_timestamp", "temperature")
  cluster_cols <- c("cluster_uuid", "cluster_status")
  
  dt <- nam_lrg |> 
    mutate(
      cluster_status = NA_character_,
      cluster_uuid = NA_character_,
      track_id = move2::mt_track_id(nam_lrg)
    ) |> 
    move2::mt_as_event_attribute(tag_id, deployment_id, individual_local_identifier, individual_id) 
  
  # post data
  ra_post_obs(
    data = dt,
    tm_id_col = mt_time_column(dt),
    additional_cols = c(store_cols, cluster_cols),
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc#,
  )
  
  # get data
  dt_retrieved <- get_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc
  ) |> 
    select(id, recorded_at, lat, lon) |> 
    mutate(
      er_obs_id = id,
      timestamp = ymd_hms(recorded_at), 
      .keep = "unused"
    )
  
  # combine data, to link observation IDS
  dt <- left_join(dt, dt_retrieved, by = c("timestamp", "lat", "lon"))
  
  ## add a mock column  -----------------
  obs_changed <- dt |> 
    mutate(mock_column = rnorm(n()),  cluster_uuid = "THIS_IS_A_CLUSTER_UUID")
  
  patch_obs(
    obs_changed |> slice(900:1001), 
    additional_cols = c(store_cols, cluster_cols, "mock_column"),
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc
  )
  
  deep_clean_obs(
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc, 
    sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
  )
  
  
})


