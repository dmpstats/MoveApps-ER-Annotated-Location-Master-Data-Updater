# Cluster Naming 
# DMP Stats
# Jun 2025

# This script will generate a unique name for each cluster using 
# a combination of the cluster's spatial and temporal elements. 
# It will work on dataframe clusters. 
# 
# Inputs ------------------------------
# obsdata | An observation-level dataset containing clusters that need to be renamed.
# xcol / ycol | The column names for the x/y coordinates. 
# timecol | The column name for the timestamp.
# cluster_id_col | The column name for the cluster ID. This can take any class 
#   that can be grouped. 
# Outputs -------------------------------
# The renamed observation data. This is the input data but the cluster ID has been 
# rewritten as a string based on the first observation's timestamp and location:
# DDMMYY-xcoord-ycoord (where xcoord/ycoord containing the x/y coordinate with 5 
# decimal places, respectively).

# Setup ----------------------

library(dplyr); library(sf); library(lubridate)

nameClusters <- function(obsdata,
                         xcol = "longitude",
                         ycol = "latitude",
                         timecol = "timestamp",
                         cluster_id_col = "cluster_id",
                         ndp = 5
) {
  
  # Validate that all named columns are within the data
  if (!all(c(xcol, ycol, timecol, cluster_id_col) %in% names(obsdata))) {
    stop("One or more specified columns do not exist in the data.")
  }
  
  # Count number of unique non-NA clusters
  num_clusters <- unique(obsdata[[cluster_id_col]]) |> 
    # na.omit() |> 
    length()
  
  # Create temporary columns for each attribute
  handledata <- obsdata |> 
    dplyr::mutate(
      XTEMPXCOL = !!rlang::sym(xcol),
      XTEMPYCOL = !!rlang::sym(ycol),
      XTEMPTIMECOL = !!rlang::sym(timecol),
      XTEMPCLUSTERIDCOL = !!rlang::sym(cluster_id_col)
    ) 
  
  
  # Convert to cluster-level
  clusterdata <- handledata |> 
    data.frame() |> # no need for sf elements
    filter(!is.na(XTEMPCLUSTERIDCOL)) |>
    group_by(XTEMPCLUSTERIDCOL) |> 
    arrange(XTEMPTIMECOL) |> 
    summarise(
      # Extract first x/y coord
      XTEMPXCOL = first(XTEMPXCOL) |> 
        as.numeric(),
      XTEMPYCOL = first(XTEMPYCOL) |> 
        as.numeric(),
      # Extract first timestamp
      XTEMPTIMECOL = first(XTEMPTIMECOL)
    ) |> 
    rowwise() |> 
    mutate(
      # We want X/Y to be exactly 5dp long
      XTEMPXCOL = sprintf(
        paste0("%.", ndp, "f"),
        # "%.5f",
        XTEMPXCOL),
      XTEMPYCOL = sprintf(
        paste0("%.", ndp, "f"),
        # "%.5f", 
        XTEMPYCOL),
      # Extract DDMMYY from timecol
      XTEMPTIMECOL = format(XTEMPTIMECOL, "%d%m%y"),
      
      # Merge into a cluster ID col
      cluster_name = paste(
        XTEMPTIMECOL, toString(XTEMPXCOL), toString(XTEMPYCOL),
        sep = "-"
      )
    ) |> 
    # Rename the cluster column to its original name
    rename(
      {{cluster_id_col}} := XTEMPCLUSTERIDCOL
    ) |> 
    select(
      {{cluster_id_col}},
      cluster_name
    ) 
  
  # Join these to the original data, overwriting the original cluster name
  obsdata <- obsdata |> 
    left_join(clusterdata, by = cluster_id_col) |> 
    select(-cluster_id_col) |> 
    rename(
      # Overwrite the original cluster name
      {{cluster_id_col}} := cluster_name
    )
  
  # Validate that num of clusters is the same
  if (n_distinct(obsdata[[cluster_id_col]]) != num_clusters) {
    stop("The number of unique clusters in the original data does not match the number of unique clusters after processing.")
  } else {
    print(sprintf("Successfully named %d clusters.", num_clusters))
  }
  
  return(obsdata)
}

# Testing 
# Generate a dataframe of clusters
# testdat <- data.frame(
#   longitude = runif(100, 0, 10),
#   latitude = runif(100, 0, 10),
#   timestamp = seq(from = as.POSIXct("2025-01-01"),
#                   to = as.POSIXct("2025-04-10"),
#                   length.out = 100),
#   cluster_id = sample(1:10, 100, replace = TRUE)
# )
# nameClusters(testdat) 
# 