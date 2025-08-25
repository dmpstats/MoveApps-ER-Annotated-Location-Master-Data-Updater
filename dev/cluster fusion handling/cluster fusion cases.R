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
dt_case1 <- readRDS("dev/cluster fusion handling/troubled_dt.rds")

matched_dt <- match_sf_clusters(
  hist_dt = dt_case1$hist_dt,
  new_dt = dt_case1$new_dt,
  cluster_id_col = "clust_id",
  timestamp_col = "timestamp",
  days_thresh = 14,
  dist_thresh = units::set_units(100, "m"),
  match_criteria = "gmedian"
)


# fusion event
fusion <- matched_dt$match_tbl |> 
  dplyr::group_by(new_cluster) |> 
  dplyr::filter(n() > 1)

fusion


# ------------------------------------------------------------ #
#         Case 2: 2 old [1 Full & 1 Partial] -> 1 new
# ------------------------------------------------------------ #
 
dt_case2 <- readRDS("dev/cluster fusion handling/troubled_dt_2.rds")

matched_dt <- match_sf_clusters(
  hist_dt = dt_case2$hist_dt,
  new_dt = dt_case2$new_dt,
  cluster_id_col = "clust_id",
  timestamp_col = "timestamp",
  days_thresh = 14,
  dist_thresh = units::set_units(100, "m"),
  match_criteria = "gmedian"
)
# 
# 
# matched_dt$matched_hist_dt |> 
#   filter(cluster_uuid == "clandestineNuptialMythoclasticYeti-20250729-075349") |> 
#   print(n = 50)
# 
# 
# # fusion event
# fusion <- matched_dt$match_tbl |>
#   dplyr::group_by(new_cluster) |>
#   dplyr::filter(n() > 1)
# 
# fusion
# 
# # fully matched hist cluster
# matched_dt$match_tbl |> 
#   dplyr::filter(master_cluster == fusion$master_cluster[[1]])
# 
# # partially matched old cluster, and it's linkage to the 2 new clusters (akin to
# # a split). Difference to a split event is the fact that one of the splits fuses
# # with another historic cluster
# matched_dt$match_tbl |> 
#   dplyr::filter(master_cluster == fusion$master_cluster[[2]])
# 
# 
# 
# hist_clusts <- matched_dt$matched_hist_dt |>
#   filter(cluster_uuid %in% fusion$master_cluster) 
# 
# hist_clusts_centroids <- hist_clusts |> 
#   group_by(cluster_uuid) |>
#   summarise(geometry = calcGMedianSF(geometry)) 
# 
# 
# 
# curr_clusts <- dt_case2$new_dt |>
#   filter(clust_id %in% c("NAM.13", "NAM.24"))  
# 
# curr_clusts_centroids <- curr_clusts |> 
#   group_by(clust_id) |>
#   summarise(geometry = calcGMedianSF(geometry)) 
# 
# 
# 
# hist_clusts |> 
#   ggplot() +
#   geom_sf(aes(colour = cluster_uuid), alpha = 0.3) +
#   geom_sf(
#     aes(colour = clust_id), 
#     data = curr_clusts, 
#     alpha = 0.3
#   ) +
#   geom_sf(data = hist_clusts_centroids, shape = 3) +
#   #geom_sf(data = curr_clusts_centroids, shape = 3) +
#   # geom_sf(
#   #   aes(colour = cluster_uuid), 
#   #   data = sf::st_buffer(hist_clusts_centroids,  units::set_units(100, "m")), 
#   #   fill = NA
#   # ) +
#   geom_sf(
#     aes(colour = clust_id), 
#     data = sf::st_buffer(curr_clusts_centroids,  units::set_units(100, "m")), 
#     fill = NA
#   ) +
#   theme(legend.position = "top")
# 
# 
# 
# merged_dt <- merge_and_update(
#   matched_dt = matched_dt,
#   new_dt = dt_case2$new_dt,
#   cluster_id_col = "clust_id",
#   timestamp_col = "timestamp",
#   store_cols = store_cols,
#   active_days_thresh = 15
# )
# 
