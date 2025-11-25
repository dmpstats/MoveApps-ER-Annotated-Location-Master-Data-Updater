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


# delete all observations and subjects in ER instance
deep_clean_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc, 
  sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
)


# activate browser() when there is an error, for interactive debugging
options(error = recover)
#options(error = NULL)



# ----------------------------------------------------------------------------------------- #
# ----        Run Workflow Locally for Namibia SOP (using ER's St Andrews Dev)          -----
# ----------------------------------------------------------------------------------------- #

## schedule run parameters
window_span <- days(20)
#window_span <- days(12)
#window_shift <- days(3)
window_shift <- days(2)
run_end_dttm <- now()
run_start_dttm <- run_end_dttm - days(90)
#run_start_dttm <- run_end_dttm - days(30)

window_ints <- tibble(
  start = seq(run_start_dttm, run_end_dttm, by = period_to_seconds(window_shift)),
  end = start + window_span
) |> 
  filter(end <= run_end_dttm + window_shift) 



#' Run for Namibia SOP Study ---------------------------------------------------------

store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", 
                "temperature", "stationary", "event_id")

nruns <- nrow(window_ints)

# initialize iteration counter
step <- 1

window_outputs <- window_ints |> 
  #slice(7:37) |> 
  pmap(function(start, end){
    
    #browser()
    
    cli::cli_rule()
    cli::cli_h1("Starting Workflow Run {step}/{nruns} @ {now()}")
    
    start_run <- now()
    
    clust_dt <<- study_level_wf(
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
      days_thresh = 14, 
      active_days_thresh = 15
    ) |> 
      cluster_metrics_app(
        cluster_id_col = "cluster_uuid",
        output_type = "cluster-based",
        cluster_tbl_type = "whole-only",
        path_to_app = apps_paths$clust_metrics
      )  |> 
      cluster_importance_app(
        map_output = FALSE, 
        path_to_app = apps_paths$clust_importance
      ) 
    
    end_run <- now()
    
    cli::cli_h2("Finished Workflow Run {step}/{nruns}. Runtime: {round(difftime(end_run, start_run, units = 'mins'), 3)} mins")
    
    # update iterating counter
    step <<- step + 1
    
    out

  })





rFunction(
  data = clust_dt,
  api_hostname = "standrews.dev.pamdas.org",
  api_token = er_tokens$standrews.dev$brunoc,
  store_cols_str = paste(store_cols, collapse = ","),
  dist_thresh = 100,
  days_thresh = 14,
  active_days_thresh = 15
)


fusion_dt_5 <- list(hist_dt = hist_dt, new_dt = data)
saveRDS(fusion_dt_5, "dev/cluster hollowing handling/fusion_dt_5.rds")
# troubled_dt_3 <- readRDS("dev/troubled_dt_3.rds")
#  
# matched_dt <- match_sf_clusters(
#   hist_dt = troubled_dt_3$hist_dt,# |> filter(cluster_uuid == "clayeyEmpathicNarcolepticPitbull-20250825-152555"),
#   new_dt = troubled_dt_3$new_dt,
#   cluster_id_col = "clust_id",
#   timestamp_col = "timestamp",
#   days_thresh = 14,
#   dist_thresh = units::set_units(100, "m"),
#   match_criteria = "gmedian"
# )
#  
# merged_dt <- merge_and_update(
#   matched_dt = matched_dt,
#   new_dt = troubled_dt_3$new_dt,
#   cluster_id_col = "clust_id",
#   timestamp_col = "timestamp",
#   store_cols = store_cols,
#   active_days_thresh = 15
# )
 

 
# # #troubled_dt <- list(hist_dt = hist_dt, new_dt = data)
# # #saveRDS(troubled_dt, "dev/troubled_dt.rds")
# troubled_dt <- readRDS("dev/cluster fusion handling/troubled_dt.rds")
#  
# matched_dt <- match_sf_clusters(
#   hist_dt = troubled_dt$hist_dt,
#   new_dt = troubled_dt$new_dt,
#   cluster_id_col = "clust_id",
#   timestamp_col = "timestamp",
#   match_criteria = "gmedian",
#   dist_thresh = units::set_units(100, "m"),
#   days_thresh = 14
# )
#  
# merged_dt <- merge_and_update(
#   matched_dt = matched_dt,
#   new_dt = troubled_dt$new_dt,
#   cluster_id_col = "clust_id",
#   timestamp_col = "timestamp",
#   store_cols = store_cols,
#   active_days_thresh = 14
# )

# Clean Up -------------------------------------

movebank_remove_credentials()

deep_clean_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc, 
  sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
)


# ------------------------------------------------------- #
# ----        Run Workflow Locally for GAIA           -----
# ------------------------------------------------------- #

## schedule run parameters
window_span <- days(30)
window_shift <- days(1)
run_end_dttm <- now()
run_start_dttm <- run_end_dttm - days(60)

window_ints <- tibble(
  start = seq(run_start_dttm, run_end_dttm, by = period_to_seconds(window_shift)),
  end = start + window_span
) |> 
  filter(end <= run_end_dttm + window_shift) 


#' Run scheduled run --------------------------------------------------

store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", 
                "temperature", "stationary", "nightpoint", "event_id")

nruns <- nrow(window_ints)

# initialize iteration counter
step <- 1

window_outputs <- window_ints |> 
  #slice(7:37) |> 
  pmap(function(start, end){
    
    #browser()
    
    cli::cli_rule()
    cli::cli_h1("Starting Workflow Run {step}/{nruns} @ {now()}")
    
    start_run <- now()
    
    clust_dt <<- study_level_wf(
      apps_paths = apps_paths,
      mvbk_usr = mvbk_creds$scavengersonpatrol$usr, 
      mvbk_pwd = mvbk_creds$scavengersonpatrol$pwd,
      study_name = 2065208399,
      animal_ids = c(
        "V002", "V004", "V005", "V006", "V007", "V008", "V009", "V011", 
        "V034", "V035", "V036", "V037", "V046", "V092"
      ),
      tm_start = start, 
      tm_end = end, 
      loc_tm_thin_mins = 3, 
      acc_tm_thin_mins = 1
    ) |> 
      cluster_app(
        clustercode = "GAIA",
        path_to_app = apps_paths$clust, 
        match_thresh = 175, 
        clustexpiration = 14
      )
    
    # need to re-source the local rFunction
    source("RFunction.R")
    
    out <- rFunction(
      data = clust_dt,
      api_hostname = "standrews.dev.pamdas.org",
      api_token = er_tokens$standrews.dev$brunoc, 
      store_cols_str = paste(store_cols, collapse = ","), 
      dist_thresh = 175,
      days_thresh = 14,   
      active_days_thresh = 16
    ) |> 
      cluster_metrics_app(
        cluster_id_col = "cluster_uuid",
        output_type = "cluster-based",
        cluster_tbl_type = "whole-only",
        path_to_app = apps_paths$clust_metrics
      )  |> 
      cluster_importance_app(
        map_output = FALSE, 
        path_to_app = apps_paths$clust_importance
      ) 
    
    end_run <- now()
    
    cli::cli_h2("Finished Workflow Run {step}/{nruns}. Runtime: {round(difftime(end_run, start_run, units = 'mins'), 3)} mins")
    
    # update iterating counter
    step <<- step + 1
    
    out
    
  })


# Clean Up -------------------------------------

movebank_remove_credentials()

deep_clean_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc, 
  sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
)




# -------------------------------------------------------------- #
# ----        Run Workflow Locally for Kendall Tanzania     -----
# -------------------------------------------------------------- #


## schedule run parameters
window_span <- days(30)
window_shift <- days(2)
run_end_dttm <- now()
run_start_dttm <- run_end_dttm - days(60)

window_ints <- tibble(
  start = seq(run_start_dttm, run_end_dttm, by = period_to_seconds(window_shift)),
  end = start + window_span
) |> 
  filter(end <= run_end_dttm + window_shift) 


#' Run scheduled run --------------------------------------------------

store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", 
                "temperature", "stationary", "nightpoint", "event_id")

nruns <- nrow(window_ints)

# initialize iteration counter
step <- 1

window_outputs <- window_ints |> 
  #slice(7:37) |> 
  pmap(function(start, end){
    
    #browser()
    
    cli::cli_rule()
    cli::cli_h1("Starting Workflow Run {step}/{nruns} @ {now()}")
    
    start_run <- now()
    
    clust_dt <<- study_level_wf(
      apps_paths = apps_paths,
      mvbk_usr = mvbk_creds$mlmackenzie$usr, 
      mvbk_pwd = mvbk_creds$mlmackenzie$pwd,
      study_name = 103394406,
      animal_ids = c(
        "C100", "C115A","C465", "ST1111A", "ST1454A", "ST1459A", "ST1460A", 
        "ST1467A", "ST1516A", "ST970A", "ST1087A", "ST1239A"
      ),
      tm_start = start, 
      tm_end = end, 
      loc_tm_thin_mins = 3, 
      acc_tm_thin_mins = 1
    ) |> 
      cluster_app(
        clustercode = "KEN_TZN",
        path_to_app = apps_paths$clust, 
        match_thresh = 175, 
        clustexpiration = 14
      )
    
    # need to re-source the local rFunction
    source("RFunction.R")
    
    out <- rFunction(
      data = clust_dt,
      api_hostname = "standrews.dev.pamdas.org",
      api_token = er_tokens$standrews.dev$brunoc, 
      store_cols_str = paste(store_cols, collapse = ","), 
      dist_thresh = 175,
      days_thresh = 14,   
      active_days_thresh = 16
    ) |> 
      cluster_metrics_app(
        cluster_id_col = "cluster_uuid",
        output_type = "cluster-based",
        cluster_tbl_type = "whole-only",
        path_to_app = apps_paths$clust_metrics
      )  |> 
      cluster_importance_app(
        map_output = FALSE, 
        path_to_app = apps_paths$clust_importance
      ) 
    
    end_run <- now()
    
    cli::cli_h2("Finished Workflow Run {step}/{nruns}. Runtime: {round(difftime(end_run, start_run, units = 'mins'), 3)} mins")
    
    # update iterating counter
    step <<- step + 1
    
    out
    
  })



# Clean Up -------------------------------------

movebank_remove_credentials()

deep_clean_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc, 
  sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
)

# ------------------------------------------------------------------------------ #
# ----        Run Workflow Locally for WhitebackedVulturesZambiaKendall     -----
# ------------------------------------------------------------------------------ #




# ------------------------------------------------- #
# ----        Run Workflow Locally for SAVAHNAH  -----
# ------------------------------------------------- #


## schedule run parameters
window_span <- days(30)
window_shift <- days(2)
run_end_dttm <- now()
run_start_dttm <- run_end_dttm - days(60)

window_ints <- tibble(
  start = seq(run_start_dttm, run_end_dttm, by = period_to_seconds(window_shift)),
  end = start + window_span
) |> 
  filter(end <= run_end_dttm + window_shift) 


#' Run scheduled run --------------------------------------------------

#store_cols <- c("behav", "local_tz", "sunrise_timestamp", "sunset_timestamp", 
#                "temperature", "stationary", "nightpoint", "event_id")

nruns <- nrow(window_ints)

# initialize iteration counter
step <- 1

clust_dt <- list()

window_outputs <- window_ints |> 
  slice(1:3) |> 
  pmap(function(start, end){
    
    #browser()
    
    cli::cli_rule()
    cli::cli_h1("Starting Workflow Run {step}/{nruns} @ {now()}")
    
    start_run <- now()
    
    clust_dt[[step]] <<- study_level_wf(
      apps_paths = apps_paths,
      mvbk_usr = mvbk_creds$mlmackenzie$usr, 
      mvbk_pwd = mvbk_creds$mlmackenzie$pwd,
      study_name = "Savannah-MEFT",
      tm_start = start, 
      tm_end = end, 
      loc_tm_thin_mins = 3, 
      acc_tm_thin_mins = 1
    ) |> 
      cluster_app(
        clustercode = "SAV",
        path_to_app = apps_paths$clust, 
        match_thresh = 100, 
        clustexpiration = 14,
        behavsystem = TRUE
      )
    
    # need to re-source the local rFunction
    source("RFunction.R")
    
    out <- rFunction(
      data = clust_dt[[step]],
      api_hostname = "standrews.dev.pamdas.org",
      api_token = er_tokens$standrews.dev$brunoc, 
      #store_cols_str = paste(store_cols, collapse = ","), 
      dist_thresh = 100,
      days_thresh = 14,   
      active_days_thresh = 16
    ) #|> 
      #cluster_metrics_app(
      #  cluster_id_col = "cluster_uuid",
      #  output_type = "cluster-based",
      #  cluster_tbl_type = "whole-only",
      #  path_to_app = apps_paths$clust_metrics
      #)  |> 
      #cluster_importance_app(
      #  map_output = FALSE, 
      #  path_to_app = apps_paths$clust_importance
      #) 
    
    end_run <- now()
    
    cli::cli_h2("Finished Workflow Run {step}/{nruns}. Runtime: {round(difftime(end_run, start_run, units = 'mins'), 3)} mins")
    
    # update iterating counter
    step <<- step + 1
    
    out
    
  })



clust_dt[[1]]
clust_dt[[2]]
clust_dt[[3]]


move2::mt_stack(
  clust_dt[[1]],
  clust_dt[[2]], .track_combine = "merge"
) |> 
  arrange(event_id) |> 
  group_by(event_id) |> 
  mutate(
    n = n()
  ) |> 
  filter(n > 1) |> 
  filter(event_id == "43102409734")
  
  
  summarise(clust_ids = length(unique(clust_id))) |> 
  filter(clust_ids > 1)


window_outputs[[1]]
window_outputs[[2]]
window_outputs[[3]]


# Clean Up -------------------------------------

movebank_remove_credentials()

deep_clean_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc, 
  sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
)



# Clean Up -------------------------------------

movebank_remove_credentials()

deep_clean_obs(
  api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/",
  token = er_tokens$standrews.dev$brunoc, 
  sources_to_keep = c("someTagID_2", "SomeUniqueIDForTheDevice", "someTagID")
)


# # Savahna
# savahn <- study_level_wf(
#   apps_paths = apps_paths,
#   mvbk_usr = mvbk_creds$mlmackenzie$usr, 
#   mvbk_pwd = mvbk_creds$mlmackenzie$pwd,
#   study_name = "Savannah-MEFT", 
#   lastXdays = 30
# ) |> 
#   cluster_app(
#     clustercode = "SAV",
#     path_to_app = apps_paths$clust, 
#     clusterstep = 5,
#     clusterwindow = 7,
#     d = 500, 
#     match_thresh = 100,
#     clustexpiration = 14, 
#     behavsystem = TRUE
#   ) 
# 





# ------------------------------------------------- #
# ----        Run Workflow Locally for WCS     -----
# ------------------------------------------------- #

  