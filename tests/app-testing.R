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

savahn <- httr2::secret_read_rds("data/raw/vult_test_data_savahn.rds", key = I(app_key))

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


deep_clean_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc, 
  sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
)



## >>>>>>>>>>>>>>>>>>>>>>>>>>
## ----   Typical Run    ----
## <<<<<<<<<<<<<<<<<<<<<<<<<<

### Set-up -----

store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature", "stationary")

nam_1mth_thin <- mt_filter_per_interval(nam_3mths, unit = "5 min") |> 
  filter(timestamp < min(timestamp) + days(70))

## schedule run parameters
window_span <- days(15)
window_shift <- days(3)
start_dttm <- min(nam_1mth_thin$timestamp)
end_dttm <- max(nam_1mth_thin$timestamp)

window_intervals <- tibble(
  start = seq(start_dttm, end_dttm, by = period_to_seconds(window_shift)),
  end = start + window_span
) |> 
  filter(end <= end_dttm + window_shift) 


### Run ----------

# initialize iteration counter
step <- 1
nruns <- nrow(window_intervals)

window_outputs <- window_intervals |> 
  #slice(1:3) |> 
  pmap(function(start, end){
    #browser()
    
    cli::cli_rule()
    cli::cli_h1("Starting Iterative Run {step}/{nruns} @ {now()}")
    
    start_run <- now()
    
    out <- nam_1mth_thin |> 
      filter(between(timestamp, start, end)) |> 
      rFunction(
        api_hostname = "standrews.dev.pamdas.org",
        api_token = er_tokens$standrews.dev$brunoc, 
        store_cols_str = paste(store_cols, collapse = ","), 
        dist_thresh = 175,
        days_thresh = 14
      )
    
    end_run <- now()
    
    cli::cli_h2("Finished Run {step}/{nruns}. Runtime: {round(difftime(end_run, start_run, units = 'mins'), 3)} mins")
    
    # update iterating counter
    step <<- step + 1
    
    Sys.sleep(2)
    
    out
  })


### checks -------------------

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
# exception comprises 3 similar cases (out of 149) where the scheduled run
# splits the original cluster into two separate clusters. Checks show deviation
# is explained by the slightly different cluster-expiration logic applied in
# the Clustering App - where the temporal cut-off is "less rigid".
# Given the choice of "14 days" for `days_thresh`, the decision to split the
# clusters appears correct.
full_join(orig_clusters, processed_clusters, by = c("spawn", "end")) |> 
  mutate(
    n_diff = n.x - n.y,
    #spawn_diff = difftime(end.x, end.y, units = "days")
  ) |> 
  print(n = 202)


# # check time gaps in input data
# nam_1mth_thin |> 
#   filter(clust_id == "NAM.45") |> 
#   mutate(timelag = difftime(timestamp, lag(timestamp), units = "days")) |> 
#   select(individual_local_identifier, timestamp, behav, clust_id, timelag ) |> 
#   print(n = 115)

# re_clust <- nam_1mth_thin |> 
#   filter(clust_id == "NAM.45") |> 
#   select(-clust_id) |> 
#   cluster_app(
#     clustercode = "NAM", 
#     match_thresh = 175, 
#     clustexpiration = 14,
#     path_to_app = apps_paths$clust
#   ) 


### Clean ER -------------------------------------

deep_clean_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc, 
  sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
)



## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## ----   Run with long lasting clusters    ----
## <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# selecting a bunch a long lasting clusters, together with typical spanning ones
nam_3mths |> 
  filter(clust_id %in% c("NAM.21", "NAM.143", "NAM.61", "NAM.58", "NAM.116")) |> 
  group_by(clust_id) |> 
  summarise(
    start_dttm = min(timestamp),
    end_dttm = max(timestamp),
    span = difftime(end_dttm, start_dttm, units = "days")
  )

### Set-up -----

nam_long_clusts <- nam_3mths |> 
  filter(clust_id %in% c( "NAM.21", "NAM.143", "NAM.61", "NAM.58", NA))

store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature", "stationary")

## schedule run parameters
window_span <- days(25)
window_shift <- days(2)
start_dttm <- min(nam_long_clusts$timestamp)
end_dttm <- max(nam_long_clusts$timestamp)

window_intervals <- tibble(
  start = seq(start_dttm, end_dttm, by = period_to_seconds(window_shift)),
  end = start + window_span
) |> 
  filter(end <= end_dttm + window_shift) 



### Run ----------

# initialize iteration counter
step <- 1
nruns <- nrow(window_intervals)

window_outputs <- window_intervals |> 
  #slice(1:3) |> 
  pmap(function(start, end){
    #browser()
    
    cli::cli_rule()
    cli::cli_h1("Starting Iterative Run {step}/{nruns} @ {now()}")
    
    start_run <- now()
    
    out <- nam_long_clusts |> 
      filter(between(timestamp, start, end)) |> 
      rFunction(
        api_hostname = "standrews.dev.pamdas.org",
        api_token = er_tokens$standrews.dev$brunoc, 
        store_cols_str = paste(store_cols, collapse = ","), 
        dist_thresh = 175,
        days_thresh = 14
      )
    
    end_run <- now()
    
    cli::cli_h2("Finished Run {step}/{nruns}. Runtime: {round(difftime(end_run, start_run, units = 'mins'), 3)} mins")
    
    # update iterating counter
    step <<- step + 1
    
    Sys.sleep(2)
    
    out
  })


### checks -------------------

# download all data in ER
dt_master <- get_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc#, 
  #created_after = run_start_dttm
)


# compare nrows
nrow(dt_master) == nrow(nam_long_clusts)

# compare cluster data
orig_clusters <- nam_long_clusts |> 
    filter(!is.na(clust_id)) |> 
    data.frame() |> 
    group_by(clust_id) |> 
    summarise(
      spawn = min(timestamp),
      end = max(timestamp),
      n = n()
    )|> 
    arrange(spawn)
  
orig_clusters

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

processed_clusters


full_join(orig_clusters, processed_clusters, by = c("spawn", "end")) |> 
  mutate(n_diff = n.x - n.y)



### Clean ER -------------------------------------

deep_clean_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc, 
  sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
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





wf_dt <- read_rds("c:/Users/Bruno/Downloads/Test_MA_ER_Master_Updater_App__Namibia_Study__Avian_Cluster_Detection__2025-07-25_15-07-03.rds")


posting_dttm <- now()

output_dt <- rFunction(
  data = wf_dt, 
  api_hostname = "standrews.dev.pamdas.org",
  api_token = er_tokens$standrews.dev$brunoc, 
  store_cols_str = NULL, 
  dist_thresh = 120
)






# ---------------------------------------- #
# ----    NC Zoo ER Instance Testing    ----
# ---------------------------------------- #


fetch_hist(api_base_url = "https://ncz-vultures-test.pamdas.org/api/v1.0/", 
            token = er_tokens$`ncz-vultures-test`$brunoc, 
            unclust_min_date = as.POSIXct("2024-03-10"), 
            include_details = TRUE,
            page_size = 5000,
            provider_key = "moveapps_ann_locs"
)



test <- get_obs(
  api_base_url = "https://ncz-vultures-test.pamdas.org/api/v1.0",
  token = "FnrVflwqlSBsD5lKRtliQhs4kX3rsm", #er_tokens$`ncz-vultures-test`$brunoc, 
  filter = 0,
  min_date = as.POSIXct("2024-03-10"), 
  max_date = as.POSIXct("2025-10-01")
)


dt2 <- readRDS("dev/misc data/Test_Merge_SA__Test_Merge_for_Southern_Africa__Cluster_Importance_Scoring__2025-11-18_05-45-36.rds")
# 
# output_dt <- rFunction(
#   data = dt1, 
#   api_hostname = "ncz-vultures-test.pamdas.org",
#   api_token = er_tokens$`ncz-vultures-test`$brunoc, 
#   store_cols_str = NULL, 
#   dist_thresh = 120
# )



# ---------------------------------------- #
# ----    St Andrews Main instance      ----
# ---------------------------------------- #

test <- get_obs(
  api_base_url = "https://standrews.pamdas.org/api/v1.0",
  token = er_tokens$standrews$brunoc, 
  filter = 0,
  min_date = lubridate::now() - lubridate::days(2)
)


fetch_hist(api_base_url = "https://standrews.pamdas.org/api/v1.0", 
           token = er_tokens$standrews$brunoc, 
           unclust_min_date = lubridate::now() - lubridate::days(2), 
           include_details = TRUE,
           page_size = 5000,
           provider_key = "moveapps_ann_locs"
)




## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## ----  Rolling window Runs     ----
## <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# Key goal here is to test the scenario where there are observations from other
# different source providers already present in ER  (e.g. data pulled directly
# from Movebank), as in the main standrews instance

### Set-up -----

#store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature", "stationary")

## schedule run parameters
window_span <- days(15)
window_shift <- days(3)
start_dttm <- min(savahn$timestamp)
end_dttm <- max(savahn$timestamp)

window_intervals <- tibble(
  start = seq(start_dttm, end_dttm, by = period_to_seconds(window_shift)),
  end = start + window_span
) |> 
  filter(end <= end_dttm + window_shift) 


### Run ----------

# initialize iteration counter
step <- 1
nruns <- nrow(window_intervals)

window_outputs <- window_intervals |> 
  #slice(1:3) |> 
  pmap(function(start, end){
    #browser()
    
    cli::cli_rule()
    cli::cli_h1("Starting Iterative Run {step}/{nruns} @ {now()}")
    
    start_run <- now()
    
    out <- savahn |> 
      filter(between(timestamp, start, end)) |> 
      rFunction(
        api_hostname = "standrews.pamdas.org",
        api_token = er_tokens$standrews$brunoc, 
        lookback = 5L,
        #store_cols_str = paste(store_cols, collapse = ","), 
        dist_thresh = 100,
        days_thresh = 14, 
        active_days_thresh = 15
      )
    
    end_run <- now()
    
    cli::cli_h2("Finished Run {step}/{nruns}. Runtime: {round(difftime(end_run, start_run, units = 'mins'), 3)} mins")
    
    # update iterating counter
    step <<- step + 1
    
    Sys.sleep(5)
    
    out
  })



### checks -------------------

# download all data in ER
dt_master <- get_obs(
  api_base_url = "https://standrews.pamdas.org/api/v1.0/",
  token = er_tokens$standrews$brunoc, 
  created_after = now() - hours(3),
  page_size = 6000
  #created_after = run_start_dttm
) |> 
  filter(
    source %in% c("7b3531ea-653c-4d66-9066-0b863d5175f2", "13c9df5d-a4a7-4932-b420-13896d0c0f64")
  )


# compare nrows
nrow(dt_master) == nrow(nam_1mth_thin)

# compare cluster data
orig_clusters <- savahn |> 
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


# full consistency between original and split-and-merged data!!!
full_join(orig_clusters, processed_clusters, by = c("spawn", "end")) |> 
  mutate(
    n_diff = n.x - n.y,
    #spawn_diff = difftime(end.x, end.y, units = "days")
  ) |> 
  print(n = 100)







 #wf_dt <- read_rds("dev/misc data/Vulture_Study_ER__Savannah_test__Avian_Cluster_Detection__2025-11-20_06-43-08.rds")
# wf_dt <- read_rds("dev/misc data/Vulture_Study_ER__Savannah_test__Avian_Cluster_Detection__2025-11-21_14-56-27.rds")
# 
# # posting_dttm <- now()
# 
# output_dt <- rFunction(
#   data = wf_dt, 
#   api_hostname = "standrews.pamdas.org",
#   api_token = er_tokens$standrews$brunoc,
#   lookback = 2L,
#   store_cols_str = NULL, 
#   dist_thresh = 100
# )


