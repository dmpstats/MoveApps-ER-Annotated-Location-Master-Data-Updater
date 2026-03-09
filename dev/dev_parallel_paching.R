# Having a go at implementing function to send PATCH requests in parallel
# ER's API only allows for single-obs PATCHing 
# Here building and testing function leveraging hhtr2's `req_perform_parallel()`
# functionality

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

set_interactive_app_testing()

# large dataset
nam_3mths <- httr2::secret_read_rds("data/raw/vult_test_data_nam3mths.rds", key = I(app_key))

# ensure no data in ER
deep_clean_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc, 
  sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
)


# ------------------------- #
#      POST data to ER
# ------------------------- #

store_cols <- c("behav", "local_tz", "sunrise_timestamp", 
                "sunset_timestamp", "temperature", "mock_col")
cluster_cols <- c("cluster_uuid", "cluster_status")


## Dataset to post under "moveapps_ann_locs" source provider
dt <- nam_3mths |> 
  mutate(
    cluster_uuid = clust_id,
    cluster_status = if_else(clust_id == "NAM.3", "CLOSED", "ACTIVE"),
    track_id = move2::mt_track_id(nam_3mths),
    mock_col = rnorm(n())
  ) |> 
  move2::mt_as_event_attribute(tag_id, deployment_id, individual_local_identifier, individual_id) |> 
  arrange(individual_local_identifier, timestamp)


expect_no_error(
  ra_post_obs(
    data = dt,
    tm_id_col = mt_time_column(dt),
    additional_cols = c(store_cols, cluster_cols),
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc#,
  )
)



# ----------------------------------------------- #
#      FETCH 20 days of historical data to ER
# ----------------------------------------------- #

final_time <- max(dt$timestamp)

dt_hist <- get_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc, 
  min_date = final_time - days(50),
  max_date = final_time - days(2)
) |> 
  #select(id, recorded_at, lat, lon) |> 
  mutate(
    er_obs_id = id,
    timestamp = ymd_hms(recorded_at), 
    .keep = "unused"
  )



dt_hist <- dt_hist |>  mutate(mock_col = rnorm(n()))


system.time(
  patch_obs(
    dt_hist, 
    additional_cols = c(store_cols, cluster_cols),
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc
  )
)

refecthved_dt <- get_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc, 
  min_date = final_time - days(22),
  max_date = final_time - days(2)
) 

identical(refecthved_dt$mock_col, dt_hist$mock_col)



# -----------------------
dt_hist <- dt_hist |>  mutate(mock_col = rnorm(n()))

system.time(
  patch_obs_parallel(
    dt_hist, 
    additional_cols = c(store_cols, cluster_cols),
    api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
    token = er_tokens$standrews.dev$brunoc
  )
)

refecthved_dt <- get_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc, 
  min_date = final_time - days(22),
  max_date = final_time - days(2)
) 

identical(refecthved_dt$mock_col, dt_hist$mock_col)







resp <- req_perform(req)
throttle_status()
resp <- req_perform(req)
throttle_status()
resp <- req_perform(req)
throttle_status()
resp <- req_perform(req)
throttle_status()
