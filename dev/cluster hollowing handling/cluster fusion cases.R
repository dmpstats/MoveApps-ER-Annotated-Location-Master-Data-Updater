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
 
dt_case2 <- readRDS("dev/cluster hollowing handling/fusion_dt_3.rds")

matched_dt <- match_sf_clusters(
  hist_dt = dt_case2$hist_dt,
  new_dt = dt_case2$new_dt,
  cluster_id_col = "clust_id",
  timestamp_col = "timestamp",
  days_thresh = 14,
  dist_thresh = units::set_units(100, "m"),
  match_criteria = "gmedian"
)
 
 
# matched_dt$matched_hist_dt |> 
#   filter(cluster_uuid == "clandestineNuptialMythoclasticYeti-20250729-075349") |> 
#   print(n = 50)
# 
# 
# fusion event
fusion <- matched_dt$match_tbl |>
  dplyr::group_by(new_cluster) |>
  dplyr::filter(n() > 1)
 
fusion
 
# fully matched hist cluster
matched_dt$match_tbl |>
  dplyr::filter(master_cluster == fusion$master_cluster[[1]])
 
# partially matched old cluster, and it's linkage to the 2 new clusters (akin to
# a split). Difference to a split event is the fact that one of the splits fuses
# with another historic cluster
matched_dt$match_tbl |>
  dplyr::filter(master_cluster == fusion$master_cluster[[2]])

 
 
hist_clusts <- matched_dt$matched_hist_dt |>
  filter(cluster_uuid %in% fusion$master_cluster)
 
hist_clusts_centroids <- hist_clusts |>
  group_by(cluster_uuid) |>
  summarise(geometry = calcGMedianSF(geometry))
 

curr_clusts <- dt_case2$new_dt |>
  filter(clust_id %in% c("NAM.24", "NAM.3"))

curr_clusts_centroids <- curr_clusts |>
  group_by(clust_id) |>
  summarise(geometry = calcGMedianSF(geometry))
 
 
 
hist_clusts |>
  ggplot() +
  geom_sf(aes(colour = cluster_uuid), alpha = 0.3) +
  # geom_sf(
  #   aes(colour = clust_id),
  #   data = curr_clusts,
  #   alpha = 0.5
  # ) +
  geom_sf(data = hist_clusts_centroids, shape = 3) +
  #geom_sf(data = curr_clusts_centroids, shape = 3) +
  # geom_sf(
  #   aes(colour = cluster_uuid),
  #   data = sf::st_buffer(hist_clusts_centroids,  units::set_units(100, "m")),
  #   fill = NA
  # ) +
  geom_sf(
    aes(colour = clust_id),
    data = sf::st_buffer(curr_clusts_centroids,  units::set_units(100, "m")),
    fill = NA
  ) +
  theme(legend.position = "top")



merged_dt <- merge_and_update(
  matched_dt = matched_dt,
  new_dt = dt_case2$new_dt,
  cluster_id_col = "clust_id",
  timestamp_col = "timestamp",
  store_cols = store_cols,
  active_days_thresh = 15
)


merged_dt |> 
  filter(cluster_uuid == "intoxicatedSlateSedateSnowdog-20250826-180340")

merged_dt |> 
  filter(clust_id == "NAM.3")




# ------------------------------------------------------------ #
#         Case 3: 2 old [2 Partials] -> 1 new
# ------------------------------------------------------------ #

dt_case3 <- readRDS("dev/cluster hollowing handling/fusion_dt_4.rds")

matched_dt <- match_sf_clusters(
  hist_dt = dt_case3$hist_dt, # |> filter(cluster_uuid %in% c("abashedLumpySpecificAnnashummingbird-20250827-004737", "ableRopeableAffordableTapir-20250827-010241")),
  new_dt = dt_case3$new_dt,# |> filter(clust_id %in% c("NAM.1", "NAM.16")),
  cluster_id_col = "clust_id",
  timestamp_col = "timestamp",
  days_thresh = 14,
  dist_thresh = units::set_units(100, "m"),
  match_criteria = "gmedian"
)


fusion <- matched_dt$match_tbl |>
  dplyr::group_by(new_cluster) |>
  dplyr::filter(n() > 1)


# 1st partially matched hist cluster
matched_dt$match_tbl |>
  dplyr::filter(master_cluster == fusion$master_cluster[[1]])

# 2nd partially matched hist cluster
matched_dt$match_tbl |>
  dplyr::filter(master_cluster == fusion$master_cluster[[3]])


hist_clusts <- matched_dt$matched_hist_dt |>
  filter(cluster_uuid %in% fusion$master_cluster)
  

hist_clusts_centroids <- hist_clusts |>
  group_by(cluster_uuid) |>
  summarise(geometry = calcGMedianSF(geometry))


curr_clusts <- dt_case3$new_dt |>
  filter(clust_id %in% c("NAM.1", "NAM.16"))

curr_clusts_centroids <- curr_clusts |>
  group_by(clust_id) |>
  summarise(geometry = calcGMedianSF(geometry))


# compare cluster points
p_hist <- hist_clusts |>
  ggplot() +
  theme(legend.position = "top") +
  geom_sf(aes(colour = cluster_uuid), alpha = 0.6)
  

p_curr <- curr_clusts |>
  ggplot() +
  theme(legend.position = "top") +
  geom_sf(aes(colour = clust_id), alpha = 0.6)
  
p_hist/p_curr




hist_clusts |>
  ggplot() +
  theme(legend.position = "top") +
  geom_sf(aes(colour = cluster_uuid), alpha = 0.6) +
  geom_sf(aes(colour = cluster_uuid), data = hist_clusts_centroids, shape = 3) +
  geom_sf(
    aes(colour = clust_id),
    data = sf::st_buffer(curr_clusts_centroids,  units::set_units(100, "m")),
    fill = NA
  )

# hist_clusts |> 
#   group_by(cluster_uuid) |> 
#   summarise(
#     start = min(recorded_at),
#     end = max(recorded_at)
#   )
# 
# 
# curr_clusts |> 
#   group_by(clust_id) |> 
#   summarise(
#     start = min(timestamp),
#     end = max(timestamp)
#   )

merged_dt <- merge_and_update(
  matched_dt = matched_dt,
  new_dt = dt_case3$new_dt,
  cluster_id_col = "clust_id",
  timestamp_col = "timestamp",
  store_cols = store_cols,
  active_days_thresh = 15
)

attributes(merged_dt)


# ------------------------------------------------------------ #
#         Case 3b: 2 old [2 Partials] -> 1 new
# ------------------------------------------------------------ #

dt_case3b <- readRDS("dev/cluster hollowing handling/fusion_dt_5.rds")

matched_dt <- match_sf_clusters(
  hist_dt = dt_case3b$hist_dt |> filter(cluster_uuid %in% c("culinarySelfawareEncyclopaedicAntarcticfurseal-20250827-130840", "impassionedWolfishRefractableWaterthrush-20250827-132804")),
  new_dt = dt_case3b$new_dt |> filter(clust_id %in% c("NAM.3", "NAM.15")),
  cluster_id_col = "clust_id",
  timestamp_col = "timestamp",
  days_thresh = 14,
  dist_thresh = units::set_units(100, "m"),
  match_criteria = "gmedian"
)


fusion <- matched_dt$match_tbl |>
  dplyr::group_by(new_cluster) |>
  dplyr::filter(n() > 1)


# 1st partially matched old cluster, and it's linkage to the 2 new clusters (akin to
# a split). Difference to a split event is the fact that one of the splits fuses
# with another historic cluster
matched_dt$match_tbl |>
  dplyr::filter(master_cluster == fusion$master_cluster[[1]])

# 2nd partially matched old cluster
matched_dt$match_tbl |>
  dplyr::filter(master_cluster == fusion$master_cluster[[2]])



hist_clusts <- matched_dt$matched_hist_dt |>
  filter(cluster_uuid %in% fusion$master_cluster)

hist_clusts_centroids <- hist_clusts |>
  group_by(cluster_uuid) |>
  summarise(geometry = calcGMedianSF(geometry))


curr_clusts <- dt_case3b$new_dt |>
  filter(clust_id %in% c("NAM.3", "NAM.15"))

curr_clusts_centroids <- curr_clusts |>
  group_by(clust_id) |>
  summarise(geometry = calcGMedianSF(geometry))


# compare cluster points
p_hist <- hist_clusts |>
  ggplot() +
  theme(legend.position = "top") +
  geom_sf(aes(colour = cluster_uuid), alpha = 0.6)


p_curr <- curr_clusts |>
  ggplot() +
  theme(legend.position = "top") +
  geom_sf(aes(colour = clust_id), alpha = 0.6)

p_hist/p_curr


merged_dt <- merge_and_update(
  matched_dt = matched_dt,
  new_dt = dt_case3b$new_dt |> filter(clust_id %in% c("NAM.3", "NAM.15")),
  cluster_id_col = "clust_id",
  timestamp_col = "timestamp",
  store_cols = store_cols,
  active_days_thresh = 15
)



# # ------------------------------------------------------------ #
# #         Case 4: 3 old [3 Partials] -> 1 new
# # ------------------------------------------------------------ #
 
dt_case4 <- readRDS("dev/cluster hollowing handling/fusion_dt_6.rds")


store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature", "stationary")

 
matched_dt <- match_sf_clusters(
  hist_dt = dt_case4$hist_dt |> filter(cluster_uuid %in% c("improvedQuasidifficultRegainableGhostshrimp-20250927-200712", "nonobjectiveFrugalEthnomusicologicalNightingale-20250927-202254", "lamproiteSuperstrictCivillawTarpan-20250927-200712")),
  new_dt = dt_case4$new_dt |> filter(clust_id %in% c("KEN_ZAM.64", "KEN_ZAM.102", "KEN_ZAM.164")),
  cluster_id_col = "clust_id",
  timestamp_col = "timestamp",
  days_thresh = 14,
  dist_thresh = units::set_units(150, "m"),
  match_criteria = "gmedian"
)

fusion <- matched_dt$match_tbl |>
  dplyr::group_by(new_cluster) |>
  dplyr::filter(n() > 1)


merged_dt <- merge_and_update(
  matched_dt = matched_dt,
  new_dt = dt_case4$new_dt,
  cluster_id_col = "clust_id",
  timestamp_col = "timestamp",
  store_cols = store_cols,
  active_days_thresh = 14
)



matched_dt$match_tbl |> 
  filter(master_cluster == "improvedQuasidifficultRegainableGhostshrimp-20250927-200712")


matched_dt$match_tbl |> 
  filter(master_cluster == "nonobjectiveFrugalEthnomusicologicalNightingale-20250927-202254")


matched_dt$match_tbl |> 
  filter(master_cluster == "lamproiteSuperstrictCivillawTarpan-20250927-200712")


hist_clusts <- matched_dt$matched_hist_dt |>
  filter(cluster_uuid %in% fusion$master_cluster)


hist_clusts_centroids <- hist_clusts |>
  group_by(cluster_uuid) |>
  summarise(geometry = calcGMedianSF(geometry))


curr_clusts <- dt_case4$new_dt |>
  filter(clust_id %in% c("KEN_ZAM.64", "KEN_ZAM.102", "KEN_ZAM.164"))

curr_clusts_centroids <- curr_clusts |>
  group_by(clust_id) |>
  summarise(geometry = calcGMedianSF(geometry))



# compare cluster points
p_hist <- hist_clusts |>
  ggplot() +
  theme(legend.position = "top") +
  geom_sf(aes(colour = cluster_uuid), alpha = 0.6)


p_curr <- curr_clusts |>
  ggplot() +
  theme(legend.position = "top") +
  geom_sf(aes(colour = clust_id), alpha = 0.6)

p_hist/p_curr






hist_clusts |>
  ggplot() +
  theme(legend.position = "top") +
  geom_sf(aes(colour = cluster_uuid), alpha = 0.6) +
  geom_sf(aes(colour = cluster_uuid), data = hist_clusts_centroids, shape = 3) +
  geom_sf(
    aes(colour = clust_id),
    data = sf::st_buffer(curr_clusts_centroids,  units::set_units(150, "m")),
    fill = NA
  ) + 
  scale_color_brewer(palette = "Set1")


