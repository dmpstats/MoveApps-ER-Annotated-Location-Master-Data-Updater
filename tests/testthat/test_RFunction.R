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



test_that("output is a valid move2 object", {
  
  actual <- rFunction(data = test_sets$wcs)
  # passes {move2} check
  expect_true(move2::mt_is_move2(actual))
  # check if 1st class is "move2"
  expect_true(class(actual)[1] == "move2")
  
})




test_that("Input validation works as expected", {
  
  testthat::local_edition(3)
  
  # missing `hostname` or `token`
  expect_snapshot(rFunction(data = test_dt$nam_1), error = TRUE)
  expect_snapshot(rFunction(data = test_dt$nam_1, api_hostname = "bla.co.uk"), error = TRUE)
  
  
  # `cluster_id_col`: unspecified or absent from input data
  expect_snapshot(
    rFunction(
      data = test_dt$nam_1, 
      api_hostname = "bla.co.uk", 
      api_token = "XYZ", 
      cluster_id_col = NULL
    ), 
    error = TRUE
  )
  
  expect_snapshot(
    rFunction(
      data = test_dt$nam_1, 
      api_hostname = "bla.co.uk", 
      api_token = "XYZ", 
      cluster_id_col = "ABSENT_COLUMN"
    ), 
    error = TRUE
  )
  
  # `lookback`
  expect_snapshot(
    rFunction(
      data = test_dt$nam_1, 
      api_hostname = "bla.co.uk", 
      api_token = "XYZ", 
      cluster_id_col = "clust_id", 
      lookback = 1.2
    ), 
    error = TRUE
  )
  
  # `store_cols_str`
  rFunction(
    data = test_dt$nam_1, api_hostname = "bla.co.uk", api_token = "XYZ",
    store_cols_str = paste("NO_COLUMN_1", "NO_COLUMN_2", "NO_COLUMN_3", "clust_id", sep = ",")
  )
  
})





