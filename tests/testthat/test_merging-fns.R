library(rlang)
library(httr2)
library(lubridate)
library(dplyr)
library(vdiffr)
library(move2)
#library(sf)
#library(here)

testthat::local_edition(3)

if(rlang::is_interactive()){
  library(testthat)
  source("tests/app-testing-helpers.r")
  set_interactive_app_testing()
  app_key <- get_app_key()
}

test_sets <- test_path("data/vult_unit_test_data.rds") |> 
  httr2::secret_read_rds(key = I(app_key)) 



# `match_sf_clusters()` -------------------------------------------------------

test_that("match_sf_clusters() fails with invalid inputs", {
  
  # wrong hist_dt
  expect_snapshot(  
    match_sf_clusters(
      hist_dt = data.frame(), 
      new_dt = slice(test_sets$nam_1, 30:100)
    ), 
    error = TRUE
  )
  
  # wrong new_dt 
  expect_snapshot(  
    match_sf_clusters(
      hist_dt = slice(test_sets$nam_1, 1:10),
      new_dt = data.frame(),
    ), 
    error = TRUE
  )
  
  # wrong cluster_id_col
  expect_snapshot(  
    match_sf_clusters(
      hist_dt = slice(test_sets$nam_1, 1:10), 
      new_dt = slice(test_sets$nam_1, 30:100), 
      cluster_id_col = "NONEXISTENT_COLUMN"
    ), 
    error = TRUE
  )
  
  
  # wrong timestamp_col
  expect_snapshot(  
    match_sf_clusters(
      hist_dt = slice(test_sets$nam_1, 1:10), 
      new_dt = slice(test_sets$nam_1, 30:100), 
      cluster_id_col = "clust_id", 
      timestamp_col = "NONEXISTENT_COLUMN"
    ), 
    error = TRUE
  )
  
  
  # missing columns in input data
  hist <- slice(test_sets$nam_1, 1:50)
  
  expect_snapshot(  
    match_sf_clusters(
      hist_dt = hist, 
      new_dt = slice(test_sets$nam_1, 30:100), 
      cluster_id_col = "clust_id", 
      timestamp_col = "timestamp"
    ), 
    error = TRUE
  )
  
  hist <- slice(test_sets$nam_1, 1:50)  |>
    mutate(cluster_uuid = sub("NAM.", "CLST_", clust_id), .keep = "unused") 
  
  expect_snapshot(  
    match_sf_clusters(
      hist_dt = hist, 
      new_dt = slice(test_sets$nam_1, 30:100), 
      cluster_id_col = "clust_id", 
      timestamp_col = "timestamp"
    ), 
    error = TRUE
  )
  
  hist <- slice(test_sets$nam_1, 1:50)  |>
    mutate(cluster_uuid = sub("NAM.", "CLST_", clust_id), .keep = "unused") |> 
    rename(recorded_at = timestamp)
  
  expect_no_error(  
    match_sf_clusters(
      hist_dt = hist, 
      new_dt = slice(test_sets$nam_1, 30:100), 
      cluster_id_col = "clust_id", 
      timestamp_col = "timestamp"
    )
  )
  
  # other inputs
  expect_snapshot(  
    match_sf_clusters(
      hist_dt = hist, 
      new_dt = slice(test_sets$nam_1, 30:100), 
      cluster_id_col = "clust_id", 
      timestamp_col = "timestamp", 
      dist_thresh = 4
    ), 
    error = TRUE
  )
  
  expect_snapshot(  
    match_sf_clusters(
      hist_dt = hist, 
      new_dt = slice(test_sets$nam_1, 30:100), 
      cluster_id_col = "clust_id", 
      timestamp_col = "timestamp", 
      match_criteria = "INVALID_CHOICE"
    ), 
    error = TRUE
  )
  
})



test_that("match_sf_clusters() works as expected", {
  
  testthat::local_edition(3)
  
  # prepare 
  hist <- test_sets$nam_1 |> 
    slice(1:50) |> 
    mutate(cluster_uuid = sub("NAM.", "CLST_", clust_id), .keep = "unused") |> 
    rename(recorded_at = timestamp) 
  
  # convert to `<sf>` 
  class(hist) <- class(hist) %>% setdiff("move2")
  
  # case 1: 2 matches (i.e 2 overlapping) + 2 new clusters
  out <- match_sf_clusters(
    hist_dt = hist, 
    new_dt = slice(test_sets$nam_1, 30:100), 
    cluster_id_col = "clust_id", 
    timestamp_col = "timestamp", 
    plot_results = TRUE)
  
  expect_snapshot(out$match_table)
  expect_snapshot(out$matched_master_data)
  expect_doppelganger("match-clusters-case-1", out$match_plot)
  
  
  # case 2: 3 non-matches, resulting from 2 non-overlapping old clusters + 1 new cluster
  out <- match_sf_clusters(
    hist_dt = hist, 
    new_dt = slice(test_sets$nam_1, 120:160), 
    cluster_id_col = "clust_id", 
    timestamp_col = "timestamp", 
    plot_results = TRUE)
  
  expect_snapshot(out$matched_master_data)
  expect_snapshot(out$match_table)
  expect_doppelganger("match-clusters-case-2", out$match_plot)
  
  
  # case 3: 2 partial matches due to old cluster being annotated as 2 separate clusters in new data
  ## forcing a "newly" detected cluster
  new <- slice(test_sets$nam_1, 40:50)
  new_trck <- move2::mt_track_data(new)
  new <- new |> 
    data.frame() |> 
    rows_update(
      tibble(event_id = as.integer64(c(39974591060, 40024755543, 40024755545)), clust_id = "NAM.6"), 
      by = "event_id"
    )
  
  new <- new |> 
    move2::mt_as_move2(, time_column = "timestamp", track_id_column = "individual_local_identifier") |> 
    mt_set_track_data(new_trck)
    
  
  out <- match_sf_clusters(
    hist_dt = hist, 
    new_dt = new, 
    cluster_id_col = "clust_id", 
    timestamp_col = "timestamp", 
    plot_results = TRUE)
  
  expect_snapshot(out$matched_master_data)
  expect_snapshot(out$match_table)
  expect_doppelganger("match-clusters-case-3", out$match_plot)
  
  
  # case 4: change distance thresh, leading to one additional non matched cluster
  out <- match_sf_clusters(
    hist_dt = hist, 
    new_dt = slice(test_sets$nam_1, 30:100), 
    cluster_id_col = "clust_id", 
    timestamp_col = "timestamp", 
    dist_thresh = units::set_units(1, "m"),
    plot_results = TRUE)
  
  expect_snapshot(out$matched_master_data)
  expect_snapshot(out$match_table)
  expect_doppelganger("match-clusters-case-4", out$match_plot)

})
  



# `merge_and_update()` -------------------------------------------------------

test_that("merge_and_update() case 1: 2 old clusters updated + 2 new clusters", {
  
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", 
                  "sunset_timestamp", "temperature")
  
  # prepare 
  hist <- test_sets$nam_1 |> 
    slice(1:50) |> 
    mt_as_event_attribute(tag_id, individual_local_identifier, individual_id) |> 
    mutate(
      cluster_uuid = sub("NAM.", "CLST_", clust_id), 
      recorded_at = timestamp,
      manufacturer_id = tag_id,
      subject_name = individual_local_identifier,
      er_obs_id = ids::uuid(n()),
      .keep = "unused"
      ) |> 
    mutate(er_source_id = ids::sentence(), .by = manufacturer_id)
  
  # convert hist to `<sf>` 
  class(hist) <- class(hist) %>% setdiff("move2")
  
  # set new data
  new <- slice(test_sets$nam_1, 30:100)
  
  # run cluster matching
  matched_hist <- match_sf_clusters(hist, new, "clust_id", "timestamp")[["matched_master_data"]]
  
  # run merging
  out <- merge_and_update(
    matched_hist_dt = matched_hist, 
    new_dt = new, 
    cluster_id_col = "clust_id", 
    timestamp_col = "timestamp", 
    store_cols = store_cols
  )

  # CLST_1 gets expanded
  expect_lt(
    sum(hist$cluster_uuid == "CLST_1", na.rm = TRUE), 
    sum(out$cluster_uuid == "CLST_1", na.rm = TRUE)
  )
  
  # CLST_2 remains unchanged
  expect_equal(
    sum(hist$cluster_uuid == "CLST_2", na.rm = TRUE), 
    sum(out$cluster_uuid == "CLST_2", na.rm = TRUE)
  )
  
  # two new clusters
  hist_clusts <- unique(na.omit(hist$cluster_uuid))
  new_clusts <- unique(na.omit(out$cluster_uuid))
  expect_length(setdiff(new_clusts, hist_clusts), 2)
  
  # number of POSTable obs equals number of new observations
  expect_equal(
    sum(out$request_type == "POST", na.rm = TRUE),
    length(setdiff(out$event_id, hist$event_id))
  )
  
})





test_that("merge_and_update() Case 2: 2 old clusters unchanged + 1 new clusters", {
  
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature")
  
  # prepare
  hist <- test_sets$nam_1 |> 
    slice(1:50) |> 
    mt_as_event_attribute(tag_id, individual_local_identifier, individual_id) |> 
    mutate(
      cluster_uuid = sub("NAM.", "CLST_", clust_id), 
      recorded_at = timestamp,
      manufacturer_id = tag_id,
      subject_name = individual_local_identifier,
      er_obs_id = ids::uuid(n()),
      .keep = "unused"
    ) |> 
    mutate(er_source_id = ids::sentence(), .by = manufacturer_id)
  
  # convert hist to `<sf>` 
  class(hist) <- class(hist) %>% setdiff("move2")
  
  # set new data
  new <- slice(test_sets$nam_1, 120:160)
  
  # run cluster matching
  matched_hist <- match_sf_clusters(hist, new, "clust_id", "timestamp")[["matched_master_data"]]
  
  # run merging
  out <- merge_and_update(
    matched_hist_dt = matched_hist, 
    new_dt = new, 
    cluster_id_col = "clust_id", 
    timestamp_col = "timestamp", 
    store_cols = store_cols
  )
  
  # CLST_1 remains unchanged
  expect_equal(
    sum(hist$cluster_uuid == "CLST_1", na.rm = TRUE), 
    sum(out$cluster_uuid == "CLST_1", na.rm = TRUE)
  )
  
  # CLST_2 remains unchanged
  expect_equal(
    sum(hist$cluster_uuid == "CLST_2", na.rm = TRUE), 
    sum(out$cluster_uuid == "CLST_2", na.rm = TRUE)
  )
  
  # 1 new clusters
  hist_clusts <- unique(na.omit(hist$cluster_uuid))
  new_clusts <- unique(na.omit(out$cluster_uuid))
  expect_length(setdiff(new_clusts, hist_clusts), 1)
  
  # number of POSTable obs equals number of new observations
  expect_equal(
    sum(out$request_type == "POST", na.rm = TRUE),
    length(setdiff(out$event_id, hist$event_id))
  )
  
})
