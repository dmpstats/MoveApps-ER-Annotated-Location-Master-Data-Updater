# update_cluster_IDs 
# DMP Stats
# Jun 2025

# We require that cluster IDs include a unique numerical component
# for example, ABC_1234, where 1234 is the unique numerical ID.
# The prefix is dropped for processing, but can be returned
# in the output. 

update_cluster_IDs <- function(
    match_table, 
    min_ID = 0, # This is the highest cluster ID that has been 'claimed'
    clustercode = "" # this is a prefix for output clusters
) {
  
  # Ensure that match_table is a df containing columns master_cluster, new_cluster and Match Type
  if (!is.data.frame(match_table) || 
      !all(c("master_cluster", "new_cluster", "Match Type") %in% colnames(match_table))) {
    cli::cli_abort("match_table must be a data frame with columns: master_cluster, new_cluster, Match Type")
  }
  
  # Preprocessing ----------------------
  
  # First, convert the master_cluster and new_cluster columns to character 
  # because it could be a factor
  match_table$rework_master_cluster <- as.character(match_table$master_cluster)
  match_table$rework_new_cluster <- as.character(match_table$new_cluster)
  
  # Next, rewrite the ID columns to remove any non-numeric characters
  match_table$rework_master_cluster <- stringr::str_replace(
    match_table$rework_master_cluster, 
    paste0("[^", clustercode, "0-9]"), 
    ""
  ) |> 
    as.numeric()
  match_table$rework_new_cluster <- stringr::str_replace(
    match_table$rework_new_cluster, 
    paste0("[^", clustercode, "0-9]"), 
    ""
  ) |> 
    as.numeric()
  
  # If the maximum value of master_cluster is equal or greater than min_ID,
  # update min_ID to the maximum value of master_cluster
  if (max(as.numeric(match_table$rework_master_cluster), na.rm = TRUE) >= min_ID) {
    min_ID <- max(as.numeric(match_table$rework_new_cluster), na.rm = TRUE) + 1
    cli::cli_alert_info("Updated min_ID to {min_ID} based on the maximum master_cluster value.")
  }
  # Identify master_clusters matched to >1 new cluster
  double_matches <- match_table$rework_master_cluster[duplicated(match_table$rework_master_cluster)] |> 
    # drop NA values from vector
    na.omit() |>
    unique()
  
  # Identify master clusters with only one match
  single_matches <- match_table$rework_master_cluster[!duplicated(match_table$rework_master_cluster)] |> 
    # drop NA values from vector
    na.omit() |>
    unique()
  
  # Append
  match_table <- match_table |> 
    dplyr::mutate(
      `Match Type` = case_when(
        rework_master_cluster %in% double_matches ~ "Partial",
        rework_master_cluster %in% single_matches ~ "Full",
        is.na(rework_master_cluster) | is.na(rework_new_cluster) ~ "No Match",
        TRUE ~ "error" # this should never be true - hopefully
      )
    )
  
  # If any errors, abort
  if (any(match_table$`Match Type` == "error")) {
    cli::cli_abort("There are errors in the match table. Please check the data.")
  }
  
  # Assign new cluster IDs ----------------------
  
  # We need a new ID for every new_cluster
  # If a master_cluster is not matched, we can happily discard it as it has no new ID
  named_matches <- match_table |> 
    dplyr::filter(!is.na(rework_new_cluster)) 
  
  # Now we handle the remaining.
  # If new_cluster has any FULL matches, we can use the most common cluster ID from this selection
  named_matches <- named_matches |> 
    dplyr::group_by(rework_new_cluster) |> 
    dplyr::summarise(
      # Take the first master_cluster ID ONLY if the Match Type is "Full"
      clustID = first(rework_master_cluster[`Match Type` == "Full"]),
      `Match Type` = first(`Match Type`)
    )  |> 
    # If clustID is NA, we want to create a new unique clustID
    # starting with the min_ID
    dplyr::mutate(
      clustID = ifelse(is.na(clustID), 
                       min_ID + row_number() - 1, 
                       clustID)
    ) 
  
  # Ensure no new ID is duplicated 
  if (any(duplicated(named_matches$clustID))) {
    cli::cli_abort("There are duplicate cluster IDs in the named matches. Please check the data.")
  }
  
  # Now we can return the match table with the new cluster IDs
  match_table <- match_table |> 
    dplyr::left_join(named_matches |> 
                       dplyr::select(rework_new_cluster, clustID), 
                     by = "rework_new_cluster") |> 
    # dplyr::mutate(
    #   new_cluster = paste0(clustercode, clustID) # Reattach the prefix
    # ) |> 
    dplyr::select(master_cluster, new_cluster, clustID, `Match Type`)
  
  # Add clustercode, if available
  if (!is.null(clustercode) && clustercode != "") {
    match_table <- match_table |> 
      dplyr::mutate(
        clustID = ifelse(
          !is.na(clustID),
          paste0(clustercode, clustID),
          NA_character_
        )
      )
  }
  
  # Return the updated match table
  return(match_table)
  
}

# # Example usage
# testdata <- readRDS("testing_output_data.rds")
# 
# update_cluster_IDs(
#   testdata$match_table,
#   clustercode = "ABC_"
#   
#   )

