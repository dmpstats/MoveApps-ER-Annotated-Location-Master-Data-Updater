# ------------------------- #
#         Preamble
# ------------------------- #

library(move2)
library(httr2)
library(purrr)
library(readr)
library(sf)

# Helpers
source("tests/app-testing-helpers.r")

# # get App secret key for decrypting test dataset
# app_key <- get_app_key()
#  
# # Read (encrypted) input datasets for testing
# test_dt <- httr2::secret_read_rds("data/raw/vult_test_data.rds", key = I(app_key))
# #map(test_dt, nrow)

set_interactive_app_testing()


test_dt <- read_rds("data/raw/input1_move2loc_LatLon.rds")


# ---------------------------------------- #
# ----    Automated Unit testing        ----
# ---------------------------------------- #

testthat::test_file("tests/testthat/test_RFunction.R")


# ---------------------------------------- #
# ----   Interactive RFunction testing  ----
# ---------------------------------------- #

out <- rFunction(data = test_dt, where = "sunny Scotland")


# ---------------------------------------- #
# ----    MoveApps SDK testing          ----
# ---------------------------------------- #

# standard dataset with default inputs
run_sdk(test_dt, where = "sunny Scotland")
  
(output <- readRDS("data/output/output.rds"))






