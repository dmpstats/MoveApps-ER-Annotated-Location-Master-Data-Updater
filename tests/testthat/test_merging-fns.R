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
  
  # out$match_table |> 
  #   dplyr::group_by(master_cluster) |> 
  #   dplyr::mutate(
  #     cluster_uuid_temp = dplyr::case_when(
  #       row_number() == 1 & !is.na(master_cluster) ~ first(master_cluster, na_rm = TRUE),
  #       row_number() > 1 & !is.na(master_cluster) ~ generate_uuid(),
  #       is.na(master_cluster) ~ generate_uuid(n())
  #     )
  #   ) |> 
  #   ungroup()
  
  
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
  
  # out$match_table |> 
  #   dplyr::group_by(master_cluster) |> 
  #   dplyr::mutate(
  #     cluster_uuid_temp = dplyr::case_when(
  #       row_number() == 1 & !is.na(master_cluster) ~ first(master_cluster, na_rm = TRUE),
  #       row_number() > 1 & !is.na(master_cluster) ~ generate_uuid(),
  #       is.na(master_cluster) ~ generate_uuid(n())
  #     )
  #   ) |> 
  #   ungroup()
  
  
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
  
  # out$match_table |> 
  #   dplyr::group_by(master_cluster) |> 
  #   dplyr::mutate(
  #     cluster_uuid_temp = dplyr::case_when(
  #       row_number() == 1 & !is.na(master_cluster) ~ first(master_cluster, na_rm = TRUE),
  #       row_number() > 1 & !is.na(master_cluster) ~ generate_uuid(),
  #       is.na(master_cluster) ~ generate_uuid(n())
  #     )
  #   ) |> 
  #   ungroup()
  
  
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
  
  
  # out$match_table |> 
  #   dplyr::group_by(master_cluster) |> 
  #   dplyr::mutate(
  #     cluster_uuid_temp = dplyr::case_when(
  #       row_number() == 1 & !is.na(master_cluster) ~ first(master_cluster, na_rm = TRUE),
  #       row_number() > 1 & !is.na(master_cluster) ~ generate_uuid(),
  #       is.na(master_cluster) ~ generate_uuid(n())
  #     )
  #   ) |> 
  #   ungroup()
  
  
})
  



# `merge_and_update()` -------------------------------------------------------


test_that("merge_and_update() Case 1: 1 cluster updated & 1 cluster unchanged + 1 new cluster", {
  
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
  new <- slice(test_sets$nam_1, 51:80)
  
  # run cluster matching
  matched_hist <- match_sf_clusters(hist, new, "clust_id", "timestamp")
  
  # run merging
  out <- merge_and_update(
    matched_dt = matched_hist,
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
  
  # CLST_5 remains unchanged
  expect_equal(
    sum(hist$cluster_uuid == "CLST_5", na.rm = TRUE), 
    sum(out$cluster_uuid == "CLST_5", na.rm = TRUE)
  )
  
  # 1 new cluster, with 1 new UUID issued
  hist_clusts <- unique(na.omit(hist$cluster_uuid))
  new_clusts <- unique(na.omit(out$cluster_uuid))
  expect_length(setdiff(new_clusts, hist_clusts), 1)
  
  # number of entrants equals number of new obs
  expect_equal(sum(out$cluster_merge_status == "ENTRANT"), nrow(new))
  
  # number of retained equals number of clustered in hist obs
  expect_equal(
    sum(out$cluster_merge_status == "RETAINED"), 
    nrow(filter(hist, !is.na(cluster_uuid)))
  )
  
  # number of POSTable obs equals number of new observations
  expect_equal(
    sum(out$request_type == "POST", na.rm = TRUE),
    length(setdiff(new$event_id, hist$event_id))
  )
  
})



test_that("merge_and_update() Case 2: 2 old clusters updated + 2 new clusters", {
  
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
  new <- slice(test_sets$nam_1, 30:100)
  
  # run cluster matching
  matched_hist <- match_sf_clusters(hist, new, "clust_id", "timestamp")
  
  # run merging
  out <- merge_and_update(
    matched_dt = matched_hist,
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
    length(setdiff(new$event_id, hist$event_id))
  )
  
})




test_that("merge_and_update() Case 3: 2 old clusters unchanged + 1 new clusters", {
  
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
  matched_hist <- match_sf_clusters(hist, new, "clust_id", "timestamp")
  
  # run merging
  out <- merge_and_update(
    matched_dt = matched_hist,
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
  
  hist_clusts <- unique(na.omit(hist$cluster_uuid))
  new_clusts <- unique(na.omit(out$cluster_uuid))
  
  # 1 new cluster
  expect_length(setdiff(new_clusts, hist_clusts), 1)
  # old clusters keep cluster ID
  expect_equal(hist_clusts, intersect(hist_clusts, new_clusts))
  
  # number of POSTable obs equals number of new observations
  expect_equal(
    sum(out$request_type == "POST", na.rm = TRUE),
    length(setdiff(out$event_id, hist$event_id))
  )
  
})




test_that("merge_and_update() Case 4: obs get dropped/recruited from/to cluster", {
  
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
    mutate(er_source_id = ids::uuid(), .by = manufacturer_id)
  
  # convert hist to `<sf>` 
  class(hist) <- class(hist) %>% setdiff("move2")
  
  # set new data
  ## (i) affiliate non-clustered to NAM.1
  ## (ii) force 2 observations to drop from cluster NAM.2
  new <- slice(test_sets$nam_1, 31:50) |>
    replace_na(list(clust_id = "NAM.1")) |> 
    mutate(
      clust_id = if_else(clust_id == "NAM.5" & row_number() %in% c(2), NA, clust_id),
      .by = clust_id
      )
  
  # run cluster matching
  matched_hist <- match_sf_clusters(hist, new, "clust_id", "timestamp")
  
  # run merging
  out <- merge_and_update(
    matched_dt = matched_hist, 
    new_dt = new, 
    cluster_id_col = "clust_id", 
    timestamp_col = "timestamp", 
    store_cols = store_cols
  )
  
  # CLST_1 expands
  expect_lt(
    sum(hist$cluster_uuid == "CLST_1", na.rm = TRUE), 
    sum(out$cluster_uuid == "CLST_1", na.rm = TRUE)
  )
  
  # CLST_5 shrinks
  expect_gt(
    sum(hist$cluster_uuid == "CLST_5", na.rm = TRUE), 
    sum(out$cluster_uuid == "CLST_5", na.rm = TRUE)
  )
  
  # 1 observations loose cluster membership
  expect_equal(sum(out$cluster_merge_status == "DROPPED", na.rm = TRUE), 1)
  # therefore losing the cluster UUID
  expect_true(out |> filter(cluster_merge_status == "DROPPED") |> pull(cluster_uuid) |> is.na() |> all())
  # 2 recruited obs
  expect_equal(sum(out$cluster_merge_status == "RECRUITED", na.rm = TRUE), 2)
  
  # which means 3 PATCHable obs
  expect_equal(sum(out$request_type == "PATCH", na.rm = TRUE), 3)
  # no POSTable obs  
  expect_equal(sum(out$request_type == "POST", na.rm = TRUE), 0)
  # no new entrants in clusters
  expect_true(nrow(filter(out, cluster_merge_status == "ENTRANT")) == 0)
  # In this case, sizes of output and hist data are equal
  expect_equal(nrow(hist), nrow(out)) 
})




test_that("merge_and_update() Case 5: cluster splits into 2 clusters", {
  
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature")
  
  # prepare
  hist <- test_sets$nam_1 |> 
    slice(1:40) |> 
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
  new <- slice(test_sets$nam_1, 30:40) |> 
    mutate(clust_id = ifelse(row_number() %in% c(2:3), "SPLIT", clust_id))
  
  # run cluster matching
  matched_hist <- match_sf_clusters(hist, new, "clust_id", "timestamp")
  
  # run merging
  out <- merge_and_update(
    matched_dt = matched_hist,
    new_dt = new, 
    cluster_id_col = "clust_id", 
    timestamp_col = "timestamp", 
    store_cols = store_cols
  )
  
  # CLST_1 shrinks (also implying its UUID is retained)
  expect_gt(
    sum(hist$cluster_uuid == "CLST_1", na.rm = TRUE), 
    sum(out$cluster_uuid == "CLST_1", na.rm = TRUE)
  )
  
  # 1 new cluster
  expect_length(setdiff(new_clusts, hist_clusts), 1)
  
  # 2 observations have been transferred
  expect_equal(sum(out$cluster_merge_status == "TRANSFERRED", na.rm = TRUE), 2)
  
  # transferred obs have changed their associated cluster UUID
  transf_obs_id <- filter(out, cluster_merge_status == "TRANSFERRED") |> pull(er_obs_id)
  expect_true(
    all(
      filter(hist, er_obs_id %in% transf_obs_id) |> pull(cluster_uuid) != 
        filter(out, er_obs_id %in% transf_obs_id) |> pull(cluster_uuid)    
    )
  )
  
  # which also means 2 PATCHable obs
  expect_equal(sum(out$request_type == "PATCH", na.rm = TRUE), 2)
  
})



test_that("merge_and_update(): CLOSED/ACTIVE cluster classification works as expected", {
  
  store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature")
  
  # prepare
  hist <- test_sets$nam_1 |> 
    slice(1:40) |> 
    mt_as_event_attribute(tag_id, individual_local_identifier, individual_id) |> 
    mutate(
      cluster_uuid = sub("NAM.", "CLST_", clust_id), 
      cluster_status = ifelse(!is.na(cluster_uuid), "ACTIVE", NA),
      recorded_at = timestamp,
      manufacturer_id = tag_id,
      subject_name = individual_local_identifier,
      er_obs_id = ids::uuid(n()),
      
      .keep = "unused"
    ) |> 
    mutate(er_source_id = ids::sentence(), .by = manufacturer_id)
  
  # convert hist to `<sf>` 
  class(hist) <- class(hist) %>% setdiff("move2")
  
  # set new data, forcing new obs to be recorded 20 later
  new <- slice_tail(test_sets$nam_2, n = 10) |> 
    mutate(timestamp = timestamp + days(20))
  
  # run cluster matching
  matched_hist <- match_sf_clusters(hist, new, "clust_id", "timestamp")
  
  # Case 1: run merging
  out <- merge_and_update(
    matched_dt = matched_hist,
    new_dt = new, 
    cluster_id_col = "clust_id", 
    timestamp_col = "timestamp", 
    store_cols = store_cols, 
    active_days_thresh = 15
  )
  
  # observations in CLST_1 changed from ACTIVE to CLOSED
  expect_equal(
    out |> filter(cluster_uuid == "CLST_1" & cluster_status == "CLOSED") |> nrow(),
    hist |> filter(cluster_uuid == "CLST_1" & cluster_status == "ACTIVE") |> nrow()
  )
  
  # which should all be labelled as patchable
  expect_equal(
    sum(out$cluster_uuid == "CLST_1", na.rm = TRUE),
    sum(out$request_type == "PATCH", na.rm = TRUE)
  )
  
  
  # Case 2: Run merging with larger threshold
  out <- merge_and_update(
    matched_dt = matched_hist,
    new_dt = new, 
    cluster_id_col = "clust_id", 
    timestamp_col = "timestamp", 
    store_cols = store_cols, 
    active_days_thresh = 25
  )
  
  # observations in CLST_1 remain ACTIVE
  expect_equal(
    out |> filter(cluster_uuid == "CLST_1" & cluster_status == "ACTIVE") |> nrow(),
    hist |> filter(cluster_uuid == "CLST_1" & cluster_status == "ACTIVE") |> nrow()
  )
  
  # which should mean no patchable obs
  expect_true(sum(out$request_type == "PATCH", na.rm = TRUE) == 0)
  
  
  
  # Case 3: Run with 4 new observations added to a closing cluster
  new <- slice_tail(test_sets$nam_2, n = 10) |> 
    mutate(timestamp = timestamp + days(20)) |> 
    move2::mt_stack(slice(test_sets$nam_1, 41:44))
  
  # run cluster matching
  matched_hist <- match_sf_clusters(hist, new, "clust_id", "timestamp")
  
  out <- merge_and_update(
    matched_dt = matched_hist,
    new_dt = new, 
    cluster_id_col = "clust_id", 
    timestamp_col = "timestamp", 
    store_cols = store_cols
  )
  
  # expect 4 new obs to be POSTed with status "CLOSED"
  expect_true(filter(out, request_type == "POST", cluster_status == "CLOSED") |> nrow() == 4)
  
  # CLST_1 expands
  expect_lt(
    sum(hist$cluster_uuid == "CLST_1", na.rm = TRUE), 
    sum(out$cluster_uuid == "CLST_1", na.rm = TRUE)
  )
  
 # number of PATCHable obs equals length of CLST_1 in hist, as all have been "CLOSED"
  expect_equal(
    sum(hist$cluster_uuid == "CLST_1", na.rm = TRUE),
    sum(out$request_type == "PATCH", na.rm = TRUE)
  )
  
  # length of closed observations and size of "CLST_1" is identical
  expect_equal(
    sum(out$cluster_status == "CLOSED", na.rm = TRUE),
    sum(out$cluster_uuid == "CLST_1", na.rm = TRUE)
  )
  
})




