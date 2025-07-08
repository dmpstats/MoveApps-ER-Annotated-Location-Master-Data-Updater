library(rlang)
library(httr2)
library(lubridate)
library(dplyr)
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




test_that("`match_sf_clusters()` works as expected", {
  
  # prepare 
  hist <- test_sets$nam_1 |> 
    slice(1:50) |> 
    mutate(cluster_uuid = sub("NAM.", "CLST_", clust_id), .keep = "unused") |> 
    rename(recorded_at = timestamp) |> 
    mutate(
      cluster_first_dttm = min(recorded_at), 
      .by = cluster_uuid
    )
  
  # convert to `<sf>` 
  class(hist) <- class(hist) %>% setdiff("move2")
  
  # 2 clusters updated + 2 new clusters
  out <- match_sf_clusters(
    hist_dt = hist, 
    new_dt = slice(test_sets$nam_1, 30:100), 
    cluster_id_col = "clust_id", 
    timestamp_col = "timestamp")
  
  expect_snapshot(out$match_table)
  
})
  