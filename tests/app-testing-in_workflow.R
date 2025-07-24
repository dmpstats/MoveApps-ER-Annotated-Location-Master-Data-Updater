# ------------------------- #
#         Preamble
# ------------------------- #

library(move2)
library(httr2)
library(purrr)
library(readr)
library(sf)
require(pak)
require(renv)
require(dplyr)
require(lubridate)
require(withr)
require(keyring)

options(dplyr.width = Inf)

# Get Helpers
source("tests/app-testing-helpers.r")
source("../../MoveApps_workflow_simulator/fcts_apps_wrappers.R")
source("../../MoveApps_workflow_simulator/helpers.r")
source("../../MoveApps_workflow_simulator/fcts_study_level_workflows.R")


# get secret keys
proj_key <- get_proj_key()
app_key <- get_app_key()


# get credentials
mvbk_creds <- httr2::secret_read_rds("../../MoveApps_workflow_simulator/mvbk_creds.rds", key = I(proj_key))
er_tokens <- httr2::secret_read_rds("dev/er_tokens.rds", key = I(app_key))


# set paths to workflow Apps
apps_paths <- list(
  mvbkloc = "../Movebank-Loc-move2/",
  localsolar = "../Convert-Times/",
  stand = "../Standardise_Formats_and_Calculate_Basic_Statistics/",
  fetch_acc = "../Fetch_and_Merge_Acceleration_to_Locations/",
  classif = "../Behavioural_Classification_for_Vultures/",
  clust = "../Avian_Cluster_Detection/",
  clust_metrics = "../Generate_Avian_Cluster_Metrics/",
  clust_importance = "../Cluster_Importance_Scoring/"
)


# # download apps' package dependencies
# apps_deps <- lapply(
#   apps_paths, function(path){
#     renv::dependencies(paste0(path, "RFunction.r"))$Package
#   }
# ) |>
#   unlist() |>
#   unique()
# 
# pak::pkg_install(apps_deps)


set_interactive_app_testing()


# -------------------------------------------------------------------- #
# ----        Run Workflow Offline  (ER's St Andrews Dev)          -----
# -------------------------------------------------------------------- #

## schedule run parameters
window_span <- days(12)
window_shift <- days(3)
run_end_dttm <- now()
run_start_dttm <- run_end_dttm - days(40)

window_ints <- tibble(
  start = seq(run_start_dttm, run_end_dttm, by = period_to_seconds(window_shift)),
  end = start + window_span
) |> 
  filter(end <= run_end_dttm + window_shift) 


nruns <- nrow(window_ints)

#' Run for Namibia SOP Study ---------------------------------------------------------

store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", 
                "temperature", "stationary", "event_id")

# initialize iteration counter
step <- 1

window_outputs <- window_ints |> 
  #slice(1:2) |> 
  pmap(function(start, end){
    
    #browser()
    
    cli::cli_rule()
    cli::cli_h1("Starting Workflow Run {step}/{nruns} @ {now()}")
    
    clust_dt <- study_level_wf(
      apps_paths = apps_paths,
      mvbk_usr = mvbk_creds$scavengersonpatrol$usr, 
      mvbk_pwd = mvbk_creds$scavengersonpatrol$pwd,
      study_name = "AVulture Namibia SOP", 
      animal_ids = c("GA_6594", "GA_6581", "GA_5404", "TO_6220", "TO_6485"), 
      tm_start = start, 
      tm_end = end, 
      loc_tm_thin_mins = 5, 
      acc_tm_thin_mins = 3
      #lastXdays = 30
    ) |> 
      cluster_app(
        clustercode = "NAM",
        path_to_app = apps_paths$clust, 
        match_thresh = 100, 
        clustexpiration = 14
      )
    
    # need to re-source the local rFunction
    source("RFunction.R")
    
    out <- rFunction(
      data = clust_dt,
      api_hostname = "standrews.dev.pamdas.org",
      api_token = er_tokens$standrews.dev$brunoc, 
      store_cols_str = paste(store_cols, collapse = ","), 
      dist_thresh = 100,
      days_thresh = 14
    ) |> 
      cluster_metrics_app(
        cluster_id_col = "cluster_uuid",
        cluster_tbl_type = "track-and-whole",
        output_type = "merge-to-locs",
        path_to_app = apps_paths$clust_metrics
      )  |> 
      cluster_importance_app(
        map_output = FALSE, 
        path_to_app = apps_paths$clust_importance
      ) 
    
    cli::cli_rule()
    cli::cli_h1("Finished Workflow Run {step}/{nruns} @ {now()}")
    
    # update iterating counter
    step <<- step + 1
    
    out

  })


# Clean Up -------------------------------------

movebank_remove_credentials()

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
  source_ids$source_id, 
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc
)  



  
window_outputs[[1]]


  