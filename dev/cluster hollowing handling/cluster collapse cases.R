# ------------------------- #
#         Preamble
# ------------------------- #

library(move2)
library(purrr)
library(readr)
library(sf)
require(renv)
require(dplyr)
require(lubridate)
require(ggplot2)
require(patchwork)

options(dplyr.width = Inf)

# Get Helpers
source("tests/app-testing-helpers.r")


# ------------------------- #
#         Case 1
# ------------------------- #

# troubled_dt_3 <- list(hist_dt = hist_dt, new_dt = data)
# saveRDS(troubled_dt_3, "dev/troubled_dt_3.rds")
collapse_case1 <- readRDS("dev/cluster hollowing handling/troubled_dt_3.rds")

matched_dt <- match_sf_clusters(
  hist_dt = collapse_case1$hist_dt,
  new_dt = collapse_case1$new_dt,
  cluster_id_col = "clust_id",
  timestamp_col = "timestamp",
  days_thresh = 14,
  dist_thresh = units::set_units(100, "m"),
  match_criteria = "gmedian"
)

merged_dt <- merge_and_update(
  matched_dt = matched_dt,
  new_dt = collapse_case1$new_dt,
  cluster_id_col = "clust_id",
  timestamp_col = "timestamp",
  store_cols = store_cols,
  active_days_thresh = 15
)




# ------------------------- #
#         Case 1
# ------------------------- #

# troubled_dt_3 <- list(hist_dt = hist_dt, new_dt = data)
# saveRDS(troubled_dt_3, "dev/troubled_dt_3.rds")
collapse_case2 <- readRDS("dev/cluster hollowing handling/fusion_dt_4.rds")

matched_dt <- match_sf_clusters(
  hist_dt = collapse_case2$hist_dt,
  new_dt = collapse_case2$new_dt,
  cluster_id_col = "clust_id",
  timestamp_col = "timestamp",
  days_thresh = 14,
  dist_thresh = units::set_units(100, "m"),
  match_criteria = "gmedian"
)

merged_dt <- merge_and_update(
  matched_dt = matched_dt,
  new_dt = collapse_case2$new_dt,
  cluster_id_col = "clust_id",
  timestamp_col = "timestamp",
  store_cols = store_cols,
  active_days_thresh = 15
)
