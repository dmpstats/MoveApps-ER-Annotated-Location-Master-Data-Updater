# ------------------------- #
#         Preamble
# ------------------------- #

library(move2)
library(httr2)
library(purrr)
library(readr)
library(sf)

options(dplyr.width = Inf)

# Helpers
source("tests/app-testing-helpers.r")

# get App secret key for decrypting test dataset
app_key <- get_app_key()

# Read (encrypted) input datasets for testing
test_dt <- httr2::secret_read_rds("data/raw/vult_test_data.rds", key = I(app_key))
#map(test_dt, nrow)

nam_3mths <- httr2::secret_read_rds("data/raw/vult_test_data_nam3mths.rds", key = I(app_key))

er_tokens <- httr2::secret_read_rds("dev/er_tokens.rds", key = I(app_key))

# activate browser() when there is an error, for interactive debugging
#options(error = recover)

#options(error = NULL)

# ---------------------------------------- #
# ----    Automated Unit testing        ----
# ---------------------------------------- #

# Functions tasked with API requests
testthat::test_file("tests/testthat/test_ER-Callers.R")

# Functions responsible for merging historical and new datasets
testthat::test_file("tests/testthat/test_merging-fns.R")

# Main rFunction
testthat::test_file("tests/testthat/test_RFunction.R")



# ----------------------------------------------- #
# ----       Simulated Scheduled testing       ----
# ----------------------------------------------- #

# This test splits a dataset into overlapping intervals and iteratively runs the
# App on each chunk. The aim is to mimic scheduled runs and assess the integrity
# of the original data after it goes through the rolling-window updating
# process. The focus is on cluster integrity, ensuring that clusters resulting
# from the iterative updates closely match those in the original dataset.

set_interactive_app_testing()

# set-up -----

store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature", "stationary")

nam_1mth_thin <- mt_filter_per_interval(nam_3mths, unit = "5 min") |> 
  filter(timestamp < min(timestamp) + days(50))

## schedule run parameters
window_span <- days(15)
window_shift <- days(2)
start_dttm <- min(nam_1mth_thin$timestamp)
end_dttm <- max(nam_1mth_thin$timestamp)

window_intervals <- tibble(
  start = seq(start_dttm, end_dttm, by = period_to_seconds(window_shift)),
  end = start + window_span
) |> 
  filter(end <= end_dttm + window_shift) 

# Run ----------

window_outputs <- window_intervals |> 
  #slice(1:3) |> 
  pmap(function(start, end){
    #browser()
    out <- nam_1mth_thin |> 
      filter(between(timestamp, start, end)) |> 
      rFunction(
        api_hostname = "standrews.dev.pamdas.org",
        api_token = er_tokens$standrews.dev$brunoc, 
        store_cols_str = paste(store_cols, collapse = ","), 
        dist_thresh = 175,
        days_thresh = 14
      )
    
    Sys.sleep(2)
    out
  }, 
  .progress = TRUE
  )

# checks -------------------

# download all data in ER
dt_master <- get_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc#, 
  #created_after = run_start_dttm
)

# compare nrows
nrow(dt_master) == nrow(nam_1mth_thin)

# compare cluster data
orig_clusters <- nam_1mth_thin |> 
  filter(!is.na(clust_id)) |> 
  data.frame() |> 
  group_by(clust_id) |> 
  summarise(
    spawn = min(timestamp),
    end = max(timestamp),
    n = n()
  )|> 
  arrange(spawn) 

processed_clusters <- dt_master |> 
  filter(!is.na(cluster_uuid)) |> 
  group_by(cluster_uuid) |> 
  mutate(recorded_at = ymd_hms(recorded_at)) |> 
  summarise( 
    spawn = min(recorded_at),
    end = max(recorded_at),
    n = n()
  ) |> 
  arrange(spawn) 


# Nearly full consistency between original and split-and-merged data. The
# exception comprises 2 similar cases (out of 149) where the scheduled run
# splits the original cluster into two separate clusters. Checks show deviation
# could be explained by slight differences between the settings specified to
# this App and those used in Clustering App used to create the original data.
# Given the choice of "14 days" for `days_thresh`, the decision to split the
# clusters appears correct.
full_join(orig_clusters, processed_clusters, by = c("spawn", "end")) |> 
  mutate(
    n_diff = n.x - n.y,
    #spawn_diff = difftime(end.x, end.y, units = "days")
  ) |> 
  print(n = 200)



# Clean ER -------------------------------------

er_sources <- get_sources(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc
)

source_ids <- purrr::keep(er_sources, \(s) s$provider == "moveapps_ann_locs") |> 
  purrr::map(~ data.frame(
    source_id = .x$id, 
    manufacturer_id = .x$manufacturer_id, 
    provider = .x$provider
  )) |> 
  list_rbind() |> 
  filter(
    provider == "moveapps_ann_locs",
    manufacturer_id %!in% c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
  )
                   
  
delete_sources(
  source_ids$source_id[1:3], 
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc
)  



# ---------------------------------------- #
# ----    MoveApps SDK testing          ----
# ---------------------------------------- #

posting_dttm <- now()

store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature", "stationary")

# default inputs
run_sdk(
  test_dt$nam_2, 
  api_hostname = "standrews.dev.pamdas.org", 
  api_token = er_tokens$standrews.dev$brunoc, 
  store_cols_str = paste(store_cols, collapse = ",")
)

(output <- readRDS("data/output/output.rds"))



# Clean up (observation-level as it's a small dataset)
pushed_test_obs <- get_obs(
  created_after = posting_dttm, 
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/", 
  token = er_tokens$standrews.dev$brunoc
)

delete_obs(pushed_test_obs$id, er_tokens$standrews.dev$brunoc)




# # ----------------------------------------- #
# # ----   Simulation-based testing   ----
# # ----------------------------------------- #
# 
# source("tests/simulate_cluster_merging.R")
# 
# 
# simulate_cluster_merging()
# 
# rFunction(
#   data = nam_1mth_thin, 
#   api_hostname = "standrews.pamdas.org",
#   api_token = "5GKEsdp0rZHgDMmCHYfswsgNYFtylt",
#   store_cols_str = paste(store_cols, collapse = ",")
# )



