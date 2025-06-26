# Sort Observations 
# DMP Stats
# Jun 2025

# This function will 'sort' the Master and Updated observation datasets into a single standardised format. 
# It will then assign ACTIVE and CLOSED cluster statuses, and finally split the data into PATCH
# and POST datasets ready to be sent to the ER server. 

# Setup --------------------------------

library(dplyr); library(lubridate)

#' Inputs -----
#'    masterdata | The 'master' dataset (from ER) with clusters reassigned by match_sf_clusters
#'    newdat | the 'new' dataset (from MA) with some cluster matching those in the masterdata. NOTE: 
#'      both datasets must contain longitude/latitude columns.
#'    cluster_id_col | the column containing cluster IDs in both datasets
#'    time_col | the column containing timestamp in both datasets
#'    animal_id_col | the column containing animal IDs in both datasets
#'    active_threshold_days | If this many days have passed between a cluster's most recent observation and the
#'        overall most recent observation, the cluster will be marked as CLOSED. Otherwise, it will be ACTIVE.
#'        Non-cluster obs will be NA.
#'  Outputs (list) --------------
#'    patch_data | A data frame containing the PATCH data, which has observation_id and source_id
#'    post_data | A data frame containing the POST data, which does not have observation_id and source_id
#'        as it does not yet exist on ER.
#'  
#'  @NOTE: I'm not yet clear on what the columns across both datasets will look like. This 
#'  script assumes consistency, but we may need to ensure this externally.
#'  

ERsplit <- function(
    masterdata,
    newdata,
    cluster_id_col = "clust_id",
    time_col = "timestamp",
    animal_id_col = "individual_local_identifier",
    active_threshold_days = 14
) {
  
  # Initialise: Check that required columns are present
  required_cols <- c(cluster_id_col, time_col, "longitude", "latitude")
  if (!all(required_cols %in% names(masterdata))) {
    stop(paste("Master data must contain the following columns:", paste(required_cols, collapse = ", ")))
  }
  if (!all(required_cols %in% names(newdata))) {
    stop(paste("New data must contain the following columns:", paste(required_cols, collapse = ", ")))
  }
  
  # Check that source_id and observation_id columns are both present in the masterdata
  # If not, this is NOT historical ER data
  if (!("source_id" %in% names(masterdata)) || !("observation_id" %in% names(masterdata))) {
    stop("Master data must contain 'source_id' and 'observation_id' columns for historical ER data.")
  }
  
  print("Initialising ERsplit")
  print(sprintf("Sorting %d masterobs and %d newobs", nrow(masterdata), nrow(newdata)))
  
  # Prepare datasets
  masterdata <- masterdata |> 
    mutate(CLUSTERTYPE = "MASTER")
  newdata <- newdata |>
    mutate(CLUSTERTYPE = "NEW")
  
  # Ensure that at least one cluster ID is present in both datasets
  # to ensure we have some form of consistency
  if (length(intersect(masterdata[[cluster_id_col]] |> na.omit() |> as.vector(),
                       newdata[[cluster_id_col]] |> na.omit() |> as.vector()
  )) == 0) {
    warning("No common cluster IDs found between master and new data. Check that matching has been effective")
  }
  
  # Column Name Handling -------------------------------
  
  # CC: We may need to extend this when implementing. Column names need to be consistent, 
  # and I'm not yet 100% sure what they'll look like at this point. For now, 
  # I'll leave it blank and assume consistency has been ensured externally.
  
  
  # Bind Datasets
  
  # Ensure that clust_id is a character vector in both datasets
  masterdata <- masterdata |> 
    mutate(!!rlang::sym(cluster_id_col) := as.character(!!rlang::sym(cluster_id_col)))
  newdata <- newdata |>
    mutate(!!rlang::sym(cluster_id_col) := as.character(!!rlang::sym(cluster_id_col)))
  alldata <- mt_stack(
    masterdata, newdata,
    .track_combine = "merge"
  ) |> 
    mutate(
      # Create temporary clusters to store essential info
      XTEMPCLUSTERCOL = !!rlang::sym(cluster_id_col),
      XTEMPTIMECOL = !!rlang::sym(time_col),
      XTEMPIDCOL = !!rlang::sym(animal_id_col)
    )
  
  
  # Now, we need to group by longitude/latitude and animal ID to identify duplicates
  spotdoubles <- alldata |> 
    group_by(
      XTEMPIDCOL,
      longitude, 
      latitude
    ) |> 
    mutate(
      # Create a unique identifier for each group
      XTEMPUNIQUEID = cur_group_id(),
      # Count the number of observations in each group
      XTEMPCOUNT = n()
    )
  
  # If any counts are 3, we have a problem. Flag a warning
  if (any(spotdoubles$XTEMPCOUNT > 2)) {
    warning(sprintf("Found %d groups with more than 2 observations. This may indicate an issue with the data.", sum(spotdoubles$XTEMPCOUNT > 2)))
  }
  
  print(sprintf("Found %d observations, of which %d have duplicates", nrow(spotdoubles), sum(spotdoubles$XTEMPCOUNT > 1)))
  
  # Whenever COUNT > 1, we have a duplicate observation which consists of 
  # an observation from ER and an observation from MoveApps. 
  # The ER observation will have a source_id and an observation_id, which we 
  # want to retain. 
  # However, we want to retain ALL other data from the ER observation, as it 
  # may have been updated. 
  joindat <- spotdoubles |> 
    group_by(
      XTEMPUNIQUEID
    ) |> 
    # Duplicate the non-NA observation_id and source_id across the whole group
    mutate(
      source_id = first(source_id[!is.na(source_id)]),
      observation_id = first(observation_id[!is.na(observation_id)])
    ) |>
    # And now only keep the ER / NEW observation in each group
    filter(
      CLUSTERTYPE == "NEW" | 
        (CLUSTERTYPE == "MASTER" & XTEMPCOUNT == 1)
    ) |> 
    arrange(
      XTEMPIDCOL,
      XTEMPTIMECOL
    )
  
  
  # Close Clusters -------------------------------------
  
  # Now we want to rewrite the cluster_status column to determine whether a cluster is 
  # ACTIVE or CLOSED. This will be determined by the most recent timestamp in all clusters:
  # if it was within active_threshold_days, then the cluster is ACTIVE, otherwise it is CLOSED.
  most_recent_obs <- newdata |> 
    ungroup() |> 
    pull(!!rlang::sym(time_col)) |>
    max(na.rm = TRUE)
  
  joindat <- joindat |> 
    group_by(
      XTEMPCLUSTERCOL
    ) |> 
    mutate(
      # Determine the most recent timestamp in the cluster
      MAXTIMESTAMP = max(XTEMPTIMECOL, na.rm = TRUE),
      # Determine if the cluster is ACTIVE or CLOSED
      cluster_status = ifelse(
        MAXTIMESTAMP >= (most_recent_obs - lubridate::days(active_threshold_days)),
        "ACTIVE",
        "CLOSED"
      ),
      # But if the cluster is NA, set it to NA
      cluster_status = ifelse(is.na(XTEMPCLUSTERCOL), NA, cluster_status)
    ) |> 
    ungroup() |> 
    select(-any_of(c("MAXTIMESTAMP", "XTEMPCOUNT", "XTEMPUNIQUEID", "XTEMPCLUSTERCOL", "XTEMPTIMECOL", "XTEMPIDCOL")))
  
  # Split ----------------------------------------
  
  # We now want to split the data into PATCH and POST data. 
  # PATCH data will be the data that has an observation_id and source_id,
  # while POST data will be the data that does not have an observation_id and source_id.
  
  patch_data <- joindat |> 
    filter(
      !is.na(observation_id) & !is.na(source_id)
    )
  post_data <- joindat |>
    filter(
      is.na(observation_id) | is.na(source_id)
    )
  print(sprintf("Split data into %d PATCH and %d POST observations", nrow(patch_data), nrow(post_data)))
  
  return(list(
    patch_data = patch_data,
    post_data = post_data
  ))
}