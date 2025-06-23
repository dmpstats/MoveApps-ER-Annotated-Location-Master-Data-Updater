library(rlang)
library(httr2)
library(lubridate)
#library(sf)
#library(here)

if(rlang::is_interactive()){
  library(testthat)
  source("tests/app-testing-helpers.r")
  set_interactive_app_testing()
  app_key <- get_app_key()
}


test_sets <- test_path("data/vult_unit_test_data.rds") |> 
  httr2::secret_read_rds(key = I(app_key)) 

er_tokens <- httr2::secret_read_rds("dev/er_tokens.rds", key = I(app_key))


test_that("rFunction() dev testing"{
  
  skip()
  
  out <- rFunction(
    data = test_dt$nam_1 |> rename(latitude = lat, longit = lon), 
    api_hostname = "https://standrews.dev.pamdas.org/", 
    api_token = er_tokens$standrews.dev$brunoc, 
    cluster_id_col = "clust_id", 
    lookback = 30L, 
    store_cols = paste0(c("clust_id", "behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", "temperature"),  collapse = ", ")
  )
  
})






test_that("output is a valid move2 object", {
  
  actual <- rFunction(data = test_sets$wcs)
  # passes {move2} check
  expect_true(move2::mt_is_move2(actual))
  # check if 1st class is "move2"
  expect_true(class(actual)[1] == "move2")
  
})

test


