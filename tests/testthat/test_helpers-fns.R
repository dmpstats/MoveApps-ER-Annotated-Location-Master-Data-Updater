library(rlang)
library(httr2)
library(lubridate)
library(move2)
library(units)
library(tibble)
#library(here)

if(rlang::is_interactive()){
  library(testthat)
  source("tests/app-testing-helpers.r")
  set_interactive_app_testing()
  app_key <- get_app_key()
  er_tokens <- httr2::secret_read_rds("dev/er_tokens.rds", key = I(app_key))
}



# is_masked() --------------

test_that("is_masked(): errors when option `what` is invalid", {
  
  # input validation
  expect_error(
    is_masked(c("true"), what = "WRONG WHAT"), 
    regexp = "`what` must be one of"
  )  
  
})


test_that("is_masked(): FALSE for empty or all-NA character vectors", {
  # empty character
  expect_false(is_masked(character(0)))
  # all NAs
  expect_false(is_masked(c(NA_character_, NA_character_)))
  # All empty strings
  expect_false(is_masked(c("", "")))
  # All empties and NAs
  expect_false(is_masked(c(NA_character_, "")))
})


test_that("is_masked(): FALSE for non-character inputs", {
  # logical
  expect_false(is_masked(c(TRUE, FALSE)))
  # factor
  expect_false(is_masked(factor("true")))
  # numeric
  expect_false(is_masked(c(1, 0)))
  # POSIXct
  expect_false(is_masked(Sys.time()))
})



### Boolean case --------
test_that("is_masked() - bool: TRUE when all non-NA character elements are whole-word 'true' or 'false' (any case)", {
  expect_true(is_masked(c("true", "FALSE"), what = "bool"))
  # NA ignored, remaining matches -> TRUE
  expect_true(is_masked(c("TRUE", "True", NA_character_), what = "bool"))
  expect_true(is_masked(c("false", "true", "FALSE"), what = "bool"))
  expect_true(is_masked(c("False", "true", ""), what = "bool"))
})



test_that("is_masked() - bool: FALSE when any non-NA element is not a whole-word match", {
  expect_false(is_masked(c("true", "no")))
  # substring is not a whole-word, so first element fails
  expect_false(is_masked(c("someFalseValue", "false"), what = "bool"))  
  expect_false(is_masked(c("nottruehere", "falsehood"), what = "bool"))
})


test_that("is_masked() - bool: FALSE for mixed NA and non-matching values", {
  # non-matching present -> FALSE
  expect_false(is_masked(c(NA_character_, "no", NA_character_), what = "bool")) 
})


### Numeric case --------
test_that("is_masked() - num: TRUE when all non-NA character elements are coercible to numeric", {
  expect_true(is_masked(c("1", "2.3", "2E3"), what = "num"))
  # NA ignored, remaining matches -> TRUE
  expect_true(is_masked(c("1", "2.3", "2E3", NA), what = "num"))
  # empty character ignored, remaining matches -> TRUE
  expect_true(is_masked(c("", "1", "2.3", "2E3", ""), what = "num"))
  # mix of NAs and empty characters ignored, remaining matches -> TRUE
  expect_true(is_masked(c(NA, "1", "2.3", "2E3", ""), what = "num"))
  # leading and trailing spaces handled and ignored
  expect_true(is_masked(c(" 234", " 77763 ", "124 "), what = "num"))
})


test_that("is_masked() - num: FALSE if ANY non-NA char element NOT coercible to numeric", {
  expect_false(is_masked(c("1", "2.3", "word1", "word2"), what = "num"))
  expect_false(is_masked(c("", "2000", "2.6", "bla", NA), what = "num"))
})





## convert_na_string() -----------------

test_that("convert_na_string() works as expected", {
  
  expect_equal(
    convert_na_string(c("1", "2", "NA", "<NA>", "3", "hello", NA)),
    c("1", "2", NA_character_, NA_character_, "3", "hello", NA_character_)
  )
  
  expect_equal(
    convert_na_string(c("NA", "<NA>", "NA", "<NA>")),
    c(NA_character_, NA_character_, NA_character_, NA_character_)
  )
  
  expect_equal(
    convert_na_string(c("hello", "world", "test")),
    c("hello", "world", "test")
  )
  
  expect_equal(convert_na_string(character(0)), character(0))
  expect_equal(convert_na_string(c(NA, NA)), c(NA, NA))
  expect_equal(convert_na_string(c(NA, "NA")), c(NA_character_, NA_character_))
  
})



## clean_obs() -----------------

test_that("clean_obs(): works as expected", {
  
  # empty dataset
  dt <- dplyr::tibble(
    a = character(0),
    b = numeric(0),
    c = logical(0)
  ) 
  
  expect_no_error(
    out <- clean_obs(dt)
  )
  
  expect_vector(out, size = 0)
  
  
  # populated dataset
  dt <- dplyr::tibble(
    a = 1:5,
    b = letters[1:5],
    c = c(NA, "2.3", "7000", "4e9", ""),
    d = c("2", "7", "thisisaword", "2.3", "2"),
    e = c(NA, "true", "false", "TRUE", ""),
    f = c("true", "nottrue", "maybefalse", "false", "nottrue"),
    g = rep(NA_character_, 5),
    h = seq.POSIXt(Sys.time(), Sys.time() + 3, length.out = 5),
    i = c("ar", "3", "NA", "<NA>", "")
  )
  
  expect_no_error(
    out <- clean_obs(dt)
  )
  
  # correctly coerced columns
  expect_true(is.numeric(out$c))
  expect_true(is.logical(out$e))
  
  # correctly skipped columns for not meeting coercing conditions
  expect_false(is.numeric(out$d))
  expect_false(is.logical(out$f))
  
  # "NA" strings replaced with true NAs
  expect_equal(out$i, c("ar", "3", NA_character_, NA_character_, ""))
  
  # correctly ignored columns
  expect_true(is.numeric(out$a))
  expect_true(is.character(out$b))
  expect_true(all(is.na(out$g)))
  expect_true(is.POSIXct(out$h))
  
})




# bind_latlon() ---------------------------------------------

test_that("bind_latlon() correctly attaches lon/lat for longlat sf object", {
  coords <- data.frame(X = c(10, 20), Y = c(-5, 15))
  pts <- st_as_sf(coords, coords = c("X", "Y"), crs = 4326)
  pts_new <- bind_latlon(pts)
  expect_true(all.equal(pts_new$lon, coords$X))
  expect_true(all.equal(pts_new$lat, coords$Y))
})


test_that("bind_latlon() ataches lon/lat but original projection is kept", {
  coords <- data.frame(X = c(500000, 400000), Y = c(4649776, 4650000))
  pts <- st_as_sf(coords, coords = c("X", "Y"), crs = 32633)
  pts_new <- bind_latlon(pts)
  
  expect_contains(names(pts_new), c("lat", "lon"))
  expect_equal(sf::st_crs(pts_new), sf::st_crs(pts))
  
  # Coordinates must be numeric lat/lon
  expect_type(pts_new$lon, "double")
  expect_type(pts_new$lat, "double")
  expect_equal(length(pts_new$lon), 2)
})


# Overwrites existing lon/lat columns
test_that("bind_latlon() overwrites existing lon/lat columns", {
  coords <- data.frame(X = 0, Y = 0, lon = NA, lat = NA)
  pts <- st_as_sf(coords, coords = c("X", "Y"), crs = 4326)
  pts_new <- bind_latlon(pts)
  expect_false(any(is.na(pts_new$lon)))
  expect_false(any(is.na(pts_new$lat)))
})
