# prepare_for_ER
# DMP Stats
# Jun 2025

# Given a set of mapped clusters, this function will merge the new-and-old cluster data
# to prepare it for ER processing. 
# Duplicates will be removed, and the data split into 'POST' and 'PATCH' datasets
# for separate handling. 

prepare_for_ER <- function(
  master_obs_mapped,
  new_obs,
  match_table,
  inactivity_threshold = NULL,
  remove_duplicates = TRUE,
  clust_id_col = "cluster"
) {
  
  # If either dataset is NULL or EMPTY, return the other - there is nothing to merge
  if (is.null(master_obs_mapped) || nrow(master_obs_mapped) == 0) {
    cli::cli_alert_danger(
      "master_obs_mapped is NULL or empty. Returning new_obs as is."
    )
    return(new_obs)
  }
  if (is.null(new_obs) || nrow(new_obs) == 0) {
    cli::cli_alert_danger(
      "new_obs is NULL or empty. Returning master_obs_mapped as is."
    )
    return(master_obs_mapped)
  }
  
  # First, ensure that the clust_id_col is in both datasets
  if (!clust_id_col %in% colnames(master_obs_mapped) || 
      !clust_id_col %in% colnames(new_obs)) {
    cli::cli_abort("Both master_obs_mapped and new_obs must contain the column: {clust_id_col}")
  }
  
  # Ensure that match_table is a df containing columns master_cluster, new_cluster and Match Type
  if (!is.data.frame(match_table) || 
      !all(c("master_cluster", "new_cluster", "Match Type") %in% colnames(match_table))) {
    cli::cli_abort("match_table must be a data frame with columns: master_cluster, new_cluster, clustID, Match Type")
  }
  
  # Preprocessing ----------------------
  
  cli::cli_inform("Preparing data for Entity Resolution (ER) processing...")
  
  # Add dependency columns
  master_obs_mapped <- master_obs_mapped |> 
    dplyr::mutate(
      new_cluster = as.character(!!rlang::sym(clust_id_col))
    )
  new_obs <- new_obs |>
    dplyr::mutate(
      new_cluster = as.character(!!rlang::sym(clust_id_col))
    )
  
  
  # Ensure that all non-NA values in master_obs_mapped are contained 
  # in match_table
  old_cluster_ids <- master_obs_mapped |> 
    dplyr::pull(new_cluster) |> 
    unique() |> 
    na.omit() |> 
    as.vector()
  if (!all(old_cluster_ids %in% match_table$master_cluster)) {
    
    cli::cli_abort("Not al master-cluster IDs are mapped to new-cluster IDs. Please check the mapping")
    
  }
  
  # Merge ---------------------
  
  cli::cli_inform("Attempting merge...")
  
  browser()
  
  # Perform the left-join, adding clustID to both datasets.
  # First the master_obs_mapped, by clustID
  master_obs_merged <- master_obs_mapped |> 
    dplyr::left_join(
      match_table |> 
        dplyr::select(
          clustID,
          master_cluster,
          new_cluster
        ),
      by = c("master_cluster", "new_cluster"),
      relationship = "many-to-one"
    ) |> 
    # rewrite the cluster_id_col as clustID's contents
    dplyr::mutate(
      !!rlang::sym(clust_id_col) := clustID
    ) |>
    # drop the master_cluster, new_cluster and clustID columns
    dplyr::select(-master_cluster, -new_cluster, -clustID)
  
  # Assign to the new_obs
  new_obs_merged <- new_obs |> 
    dplyr::left_join(
      match_table |> 
        dplyr::select(
          clustID,
          new_cluster
        ) |> 
        distinct(),
      by = "new_cluster",
      relationship = "many-to-one"
    ) |> 
    # rewrite the cluster_id_col as clustID's contents
    dplyr::mutate(
      !!rlang::sym(clust_id_col) := clustID
    ) |>
    # drop the new_cluster and clustID columns
    dplyr::select(-new_cluster, -clustID)
  
  # Handle duplicates ----------------------
  
  if (remove_duplicates) {
    cli::cli_alert_warning("Duplicate handling not yet written. Don't forget to do this...")
  }
  
  
  # Handle 'active' clusters  -----------------
  
  if (!is.null(inactivity_threshold) && inactivity_threshold > 0) {
    cli::cli_alert_warning("Inactivity threshold not yet written. Don't forget to do this...")
    
    # If any clusterrs were last active over 'inactivity_threshold' days ago, we log them as INACTIVE. 
    # This means we do not pull them the next time we call from ER
  }
  
  # for now 
  master_obs_merged$active_cluster <- TRUE
  new_obs_merged$active_cluster <- TRUE
  
  # Split into POST and PATCH datasets ----------------------
  
  cli::cli_inform("Splitting data into POST and PATCH datasets...")
  
  cli::cli_alert_warning("POST and PATCH datasets not yet written. Don't forget to do this...")
  
  # We need to split into:
  # - POST data: new observations that don't exist on the ER sever
  # - PATCH data: data that exists on the ER server and needs just one attribute
  # (cluster ID) rewritten
  
}

prepare_for_ER(
  master_obs_mapped = result$matched_master_data,
  new_obs = newclusters,
  match_table = id_map
)
