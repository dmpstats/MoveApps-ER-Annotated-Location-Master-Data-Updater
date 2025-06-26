# match_sf_clusters
# DMP Stats
# Jun 2025

# This function is the workhorse for matching master clusters to their updated counterparts. 
# The updating of IDs is not handled here (yet).


# dependencies
library(sf); library(dplyr); library(ggplot2)

# Gmedian helper
calcGMedianSF <- function(data) {
  
  if (st_geometry_type(data[1,]) == "POINT") {
    
    med <- data %>% 
      st_coordinates()
    
    med <- Gmedian::Weiszfeld(st_coordinates(data))$median %>% as.data.frame() %>%
      rename(x = V1, y = V2) %>%
      st_as_sf(coords = c("x", "y"), crs = st_crs(data)) %>%
      st_geometry()
  }
  
  
  if (st_geometry_type(data[1,]) == "MULTIPOINT") {
    
    med <- data %>% 
      st_coordinates() %>%
      as.data.frame() %>%
      group_by(L1) %>%
      group_map( ~st_point(Gmedian::Weiszfeld(.)$median) ) |> 
      st_as_sfc(crs = st_crs(data))
    
  }
  
  return(med)
  
}


# CLUSTER MATCH FUNCTION ---------------------------------

# Inputs ------
# masterclusters | the master-cluster dataset from ER.
# newclusters | the 'updated' cluster dataset from MA. 
# cluster_id_col | The name of the cluster ID column in both datasets.
# timestamp_col | The name of the timestamp column in both datasets.
# thresh_days | The time difference between two cluster-intervals that must not be 
#     exceeded for these clusters to be merged. 
# matching_threshold | The maximum distance (m) between two cluster centroids/medians 
#     for them to be 'matched'.
# matching_criteria | Whether to perform the merging based on centroids or 
#     geometric medians.
# plot_results | if TRUE, cluster-matching plots will be generated as a diagnostic.

match_sf_clusters <- function(masterclusters,
                              newclusters,
                              cluster_id_col = "cluster",
                              timestamp_col = "timestamp",
                              thresh_days = 7,
                              matching_threshold = units::set_units(100, "metres"),
                              matching_criteria = c("gmedian", "centroid"),
                              plot_results = TRUE
) {
  
  print("Matching Spatial Clusters")
  
  # Input validation ----------------------
  
  # First, check that the new dataset is non-null and non-empty
  if (is.null(newclusters) || nrow(newclusters) == 0) {
    stop("New clusters dataset is null or empty.")
  }
  
  # If the old clusters are null or empty, return the new clusters 
  if (is.null(masterclusters) || nrow(masterclusters) == 0) {
    stop("Master cluster dataset is null or empty.")
    # return(newclusters)
  }
  
  # Check that matching_criteria is valid
  if (!(matching_criteria %in% c("gmedian", "centroid"))) {
    stop("Invalid matching criteria. Use 'gmedian' or 'centroid'.")
  }
  
  # Check that the cluster_id_col exists in both datasets
  if (!cluster_id_col %in% names(masterclusters) || !cluster_id_col %in% names(newclusters)) {
    stop(sprintf("Cluster ID column '%s' not found in both datasets.", cluster_id_col))
  }
  
  # Check that the timestamp_col exists in both datasets
  if (!timestamp_col %in% names(masterclusters) || !timestamp_col %in% names(newclusters)) {
    stop(sprintf("Timestamp column '%s' not found in both datasets.", timestamp_col))
  }
  
  # Convert coord. system of old to new
  masterclusters <- sf::st_transform(masterclusters, sf::st_crs(newclusters))
  
  # Valid. Proceed
  print(sprintf(
    "Matching %d new clusters to %d master clusters.",
    length(unique(newclusters[[cluster_id_col]])), length(unique(masterclusters[[cluster_id_col]]))
  ))
  print(sprintf(
    "Matching criteria: %s, threshold: %s.", matching_criteria, as.character(matching_threshold)
  ))
  
  # Reformat data as necessary ---------------------
  
  masterclusters <- masterclusters |> 
    dplyr::mutate(
      XTEMPCLUSTER = as.character(!!rlang::sym(cluster_id_col))
    ) 
  newclusters <- newclusters |>
    dplyr::mutate(
      XTEMPCLUSTER = as.character(!!rlang::sym(cluster_id_col))
    )
  
  # If timestamp_col is not NULL, create a temporary column for the timestamp
  if (!is.null(timestamp_col)) {
    masterclusters <- masterclusters |> 
      dplyr::mutate(
        XTIMESTAMP = as.POSIXct(!!rlang::sym(timestamp_col), tz = "UTC")
      )
    newclusters <- newclusters |>
      dplyr::mutate(
        XTIMESTAMP = as.POSIXct(!!rlang::sym(timestamp_col), tz = "UTC")
      )
  } else {
    # create empty columns
    masterclusters <- masterclusters |> 
      dplyr::mutate(XTIMESTAMP = NA)
    newclusters <- newclusters |>
      dplyr::mutate(XTIMESTAMP = NA)
  }
  
  
  # Determine centroids --------------------
  
  print(sprintf("Calculating centroids for old and new clusters based on matching criteria: %s.", matching_criteria))
  
  # If gmedian, calculate the geometric median
  if (matching_criteria == "gmedian") {
    master_centroids <- masterclusters |> 
      dplyr::filter(!is.na(XTEMPCLUSTER)) |>
      dplyr::group_by(XTEMPCLUSTER) |> 
      dplyr::summarise(
        geometry = calcGMedianSF(geometry), .groups = "drop",
        starttime = min(XTIMESTAMP, na.rm = TRUE),
        endtime = max(XTIMESTAMP, na.rm = TRUE)
      )
    new_centroids <- newclusters |>
      dplyr::filter(!is.na(XTEMPCLUSTER)) |>
      dplyr::group_by(XTEMPCLUSTER) |> 
      dplyr::summarise(
        geometry = calcGMedianSF(geometry), .groups = "drop",
        starttime = min(XTIMESTAMP, na.rm = TRUE),
        endtime = max(XTIMESTAMP, na.rm = TRUE)
      )
  } else if (matching_criteria == "centroid") {
    # If centroid, calculate the centroid
    master_centroids <- masterclusters |> 
      dplyr::filter(!is.na(XTEMPCLUSTER)) |>
      dplyr::group_by(XTEMPCLUSTER) |> 
      dplyr::summarise(
        geometry = sf::st_combine(geometry), .groups = "drop",
        starttime = min(XTIMESTAMP, na.rm = TRUE),
        endtime = max(XTIMESTAMP, na.rm = TRUE)
      ) |> 
      sf::st_centroid()
    new_centroids <- newclusters |>
      dplyr::filter(!is.na(XTEMPCLUSTER)) |>
      dplyr::group_by(XTEMPCLUSTER) |> 
      dplyr::summarise(
        geometry = sf::st_combine(geometry), .groups = "drop",
        starttime = min(XTIMESTAMP, na.rm = TRUE),
        endtime = max(XTIMESTAMP, na.rm = TRUE)
      ) |> 
      sf::st_centroid()
  }
  
  # Match centroids ------------------
  
  # Buffer the new centroids by the matching threshold
  new_centroids_buffered <- sf::st_buffer(new_centroids, 
                                          dist = matching_threshold
  )
  
  # Now we want a data.frame of matches. Left column contains any master cluster ID, 
  # right column contains any matching new cluster ID, with multiple entries for 
  # multiple matches.
  matches <- sf::st_join(master_centroids, new_centroids_buffered, join = sf::st_intersects) |>
    data.frame() |> 
    dplyr::select(XTEMPCLUSTER.x, XTEMPCLUSTER.y,
                  starttime.x, endtime.x,
                  starttime.y, endtime.y
    ) |>
    dplyr::rename(
      master_cluster = XTEMPCLUSTER.x,
      master_start = starttime.x,
      master_end = endtime.x,
      new_cluster = XTEMPCLUSTER.y,
      new_start = starttime.y,
      new_end = endtime.y
    ) #|>
  # dplyr::filter(!is.na(master_cluster) & !is.na(new_cluster)) # Remove any rows where no match was found
  
  # If we have a timestamp column AND a valid matching threshold, 
  # remove any matches that are outside the threshold
  if (!is.null(timestamp_col) && !is.na(thresh_days) && thresh_days > 0) {
    time_matches <- matches |> 
      # We buffer the interval by half of thresh_days (rounded) and check overlap
      mutate(
        master_interval = lubridate::interval(master_start - lubridate::days(floor(thresh_days/2)), master_end + lubridate::days(ceiling(thresh_days/2))),
        new_interval = lubridate::interval(new_start - lubridate::days(floor(thresh_days/2)), new_end + lubridate::days(ceiling(thresh_days/2))),
        VALID_TIME = lubridate::int_overlaps(master_interval, new_interval)
      ) |> 
      
      # If the time match is invalid, delete the entry
      filter(!is.na(VALID_TIME) | VALID_TIME) |> 
      select(master_cluster, new_cluster)  
    print(sprintf(
      "Removing %d matches that do not meet the time threshold of %d days.",
      nrow(matches) - nrow(time_matches), thresh_days
    ))
    
    # And add a new entry for any master_clusters missing
    missing_clusters <- masterclusters$XTEMPCLUSTER[!masterclusters$XTEMPCLUSTER %in% time_matches$master_cluster] |> 
      unique() |> 
      # drop NA
      na.omit() |> 
      as.vector() 
    
    if (length(missing_clusters) > 0) {
      missing_clusters_df <- data.frame(
        master_cluster = missing_clusters,
        new_cluster = NA_character_
      )
      matches <- dplyr::bind_rows(time_matches, missing_clusters_df)
    } # debug
  } else {
    # If not handling times, just get down to the essential columns
    matches <- matches |> 
      dplyr::select(master_cluster, new_cluster) |> 
      dplyr::distinct() 
  }
  
  
  
  # Filter to double-matches, where an old cluster is matched to multiple new clusters
  double_matches <- matches |>
    dplyr::group_by(master_cluster) |>
    dplyr::filter(dplyr::n() > 1
    ) |>
    dplyr::ungroup()
  direct_matches <- matches |>
    dplyr::filter(!(master_cluster %in% double_matches$master_cluster)) 
  
  nonmatches <- matches |> 
    filter(!is.na(master_cluster) & is.na(new_cluster)) |> 
    pull(master_cluster) 
  print(sprintf(
    "Found %d master clusters that do not match any new clusters.", length(nonmatches)
  ))
  
  if (any(nonmatches %in% double_matches$master_cluster)) {
    stop("Some master clusters simultaneously have no matches and double matches. This is conflicting.")
  }
  
  print(sprintf(
    "Found %d matches, of which %d are double-matches.", nrow(matches), nrow(double_matches)
  ))
  
  # Performing match ---------------------
  
  # Resolve matches down to observation level 
  matched_old_obs <- masterclusters |> 
    dplyr::left_join(direct_matches, by = c("XTEMPCLUSTER" = "master_cluster")) |> 
    # Get the nearest ID for ALL clusters
    dplyr::mutate(
      nearest_new_cluster = new_centroids$XTEMPCLUSTER[st_nearest_feature(geometry, new_centroids)]
    ) |> 
    # If the old cluster is in double_matches, reassociate the new cluster to nearest_new_cluster
    dplyr::mutate(
      new_cluster = ifelse(XTEMPCLUSTER %in% double_matches$master_cluster,
                           nearest_new_cluster,
                           new_cluster
      )
    ) |> 
    # If the old cluster is in nonmatches, we need to create a new temporary cluster ID
    group_by(XTEMPCLUSTER) |> 
    
    # If the old cluster is in nonmatches, assign a new cluster ID generated for the group
    dplyr::mutate(
      new_cluster = ifelse(XTEMPCLUSTER %in% nonmatches,
                           paste0("UNMATCHED.", dplyr::cur_group_id()),
                           new_cluster
      )
    ) |>
    ungroup() |> 
    
    dplyr::select(-all_of("nearest_new_cluster")) 
  
  # Create a lookup table for matches to output
  final_matches <- matched_old_obs |> 
    data.frame() |> 
    dplyr::select(c("XTEMPCLUSTER", "new_cluster")) |>
    dplyr::rename(
      master_cluster = XTEMPCLUSTER
    ) |>
    dplyr::mutate(
      # This is Partial if the old cluster is in double_matches,
      # otherwise it is Full
      `Match Type` = ifelse(master_cluster %in% double_matches$master_cluster, "Partial", "Full"),
      # If either cluster column is NA, overwrite `Match Type` as "No Match"
      # `Match Type` = ifelse(is.na(old_cluster) | is.na(new_cluster), "No Match", `Match Type`)
      `Match Type` = ifelse(is.na(new_cluster), "No Match", `Match Type`),
      # If the master cluster is nonmatched, mark it 
      `Match Type` = ifelse(master_cluster %in% nonmatches, "No Match", `Match Type`)
    ) |> 
    dplyr::distinct() # Ensure distinct matches
  
  # Add to final_matches any new clusters that have no previous match
  unmatched_new_clusters <- new_centroids |> 
    data.frame() |> 
    dplyr::filter(!XTEMPCLUSTER %in% final_matches$new_cluster) |> 
    dplyr::mutate(
      master_cluster = NA_character_,
      `Match Type` = "No Match"
    ) |> 
    dplyr::select(master_cluster, new_cluster = XTEMPCLUSTER, `Match Type`)
  
  print(sprintf(
    "Found %d unmatched new clusters. Adding to matchtable", nrow(unmatched_new_clusters)
  ))
  
  final_matches <- final_matches |>
    dplyr::bind_rows(unmatched_new_clusters) |>
    dplyr::filter(!is.na(master_cluster) | !is.na(new_cluster)) |> # to get rid of NA -> NA
    dplyr::distinct() # Ensure distinct matches
  
  
  # Give the cluster column back its original name
  outdata <- matched_old_obs |> 
    dplyr::mutate(
      !!cluster_id_col := new_cluster
    ) |> 
    dplyr::rename(master_cluster = XTEMPCLUSTER) |> 
    dplyr::select(
      -any_of(c("XTEMPCLUSTER", "XTIMESTAMP", "new_cluster"))
    )
  
  
  # Plots ------------------
  
  # If plot_results is TRUE, we want a plot of arrows 
  # originating from the old cluster 
  # and pointing to the new cluster
  if (plot_results) {
    library(ggplot2)
    
    print("Preparing output cluster-matching plots")
    
    # Create a plot of the old and new clusters
    old_coords <- master_centroids |> 
      cbind(st_coordinates(master_centroids)) |> 
      as.data.frame() |> 
      dplyr::rename(
        master_cluster = XTEMPCLUSTER,
        Xold = X, Yold = Y
      ) |> 
      dplyr::select(
        all_of(c("master_cluster", "Xold", "Yold"))
      )
    new_coords <- new_centroids |> 
      cbind(st_coordinates(new_centroids)) |> 
      as.data.frame() |> 
      dplyr::rename(
        new_cluster = XTEMPCLUSTER,
        Xnew = X, Ynew = Y
      ) |> 
      dplyr::select(
        all_of(c("new_cluster", "Xnew", "Ynew"))
      )
    
    # Join these to the final_matches
    plot_matches <- final_matches |> 
      dplyr::left_join(old_coords, by = "master_cluster") |> 
      dplyr::left_join(new_coords, by = "new_cluster")
    
    # And plot out
    # Top-level cluster plotting
    matchplot <- ggplot() + 
      theme_bw() +
      geom_sf(data = new_centroids_buffered |> dplyr::filter(XTEMPCLUSTER %in% plot_matches$new_cluster), fill = NA, alpha = 0.15, linewidth = 0.1,
              aes(color = "New Cluster", linetype = "Connection Radius")
      ) +
      geom_segment(data = plot_matches |> dplyr::filter(!is.na(new_cluster) & !is.na(master_cluster)),
                   aes(x = Xold, y = Yold, xend = Xnew, yend = Ynew, linetype = `Match Type`), linewidth = 0.6,
                   arrow = arrow(length = unit(0.2, "cm")), size = 0.5) +
      geom_label(data = plot_matches |> 
                   dplyr::filter(!is.na(master_cluster)), 
                 aes(x = Xold, y = Yold, 
                     label = master_cluster, 
                     color = "Master Cluster",
                     fill = `Match Type`
                 ), 
                 size = 3) +
      geom_label(data = plot_matches |> 
                   dplyr::filter(!is.na(new_cluster)),
                 aes(x = Xnew, y = Ynew, 
                     label = new_cluster, 
                     color = "New Cluster",
                     fill = `Match Type`
                 ), 
                 size = 3) +
      scale_color_manual(
        name = "Cluster Type",
        values = c("Match" = "black", "Master Cluster" = "blue", "New Cluster" = "red")) +
      scale_fill_manual(
        name = "Unmatched Clusters",
        values = c("Full" = "white", "Partial" = "white", "No Match" = "yellow")) +
      scale_linetype_manual(
        name = "Match Type",
        values = c("Full" = "solid", "Partial" = "dashed", "Connection Radius" = "dotted")) +
      xlab("X") + ylab("Y") +
      ggtitle("Cluster association map")
    # coord_fixed()
    
    # Point-level cluster plotting
    prep_pts <- outdata |> 
      cbind(sf::st_coordinates(outdata)) |> 
      data.frame() |> 
      dplyr::rename(
        XTEMPCLUSTER = !!rlang::sym(cluster_id_col),
        pointX = X, pointY = Y) |> 
      # dplyr::filter(!is.na(XTEMPCLUSTER) & XTEMPCLUSTER != 0) |> 
      dplyr::left_join(
        new_centroids |>
          cbind(st_coordinates(new_centroids)) |>
          data.frame() |>
          rename(clusterX = X, clusterY = Y),
        by = "XTEMPCLUSTER"
      ) |> 
      dplyr::left_join(
        master_centroids |>
          cbind(st_coordinates(master_centroids)) |>
          data.frame() |>
          dplyr::filter(!is.na(XTEMPCLUSTER) & XTEMPCLUSTER != 0) |> 
          rename(clusterXold = X, clusterYold = Y,
                 master_cluster = XTEMPCLUSTER
          ),
        by = "master_cluster"
      ) 
    
    pointplot <- ggplot() + 
      theme_bw() +
      geom_segment(
        data = prep_pts |> dplyr::filter(!is.na(master_cluster)), 
        aes(
          x = pointX, y = pointY,
          xend = clusterXold, yend = clusterYold, color = "Master"
        ),
        linetype = "dashed"
      ) +
      geom_segment(
        data = prep_pts |> dplyr::filter(!is.na(master_cluster) & !is.na(XTEMPCLUSTER)),
        aes(
          x = pointX, y = pointY,
          xend = clusterX, yend = clusterY, color = "New"
        )
      ) +
      geom_point(
        data = prep_pts |> dplyr::filter(is.na(XTEMPCLUSTER)), 
        aes(x = pointX, y = pointY, color = "No Cluster"),
        shape = 4
      ) +
      scale_color_manual(
        name = "Cluster Type",
        values = c("Master" = "blue", "New" = "red", "No Cluster" = "black")
      ) +
      coord_fixed() + 
      xlab("X") + ylab("Y") +
      ggtitle("Master Clusters [mapped to new centroids]")
    
    allplot <- cowplot::plot_grid(
      matchplot, pointplot,
      ncol = 2, 
      labels = c("A", "B"),
      label_size = 12
    )
    
  } else {allplot <- NULL}
  
  
  print(sprintf(
    "Matched %d clusters from old data to new data.", nrow(final_matches)
  ))
  
  # Return list 
  return(
    list(
      matched_master_data = outdata,
      match_table = final_matches,
      match_plot = allplot
    )
  )
  
}

# Example usage (uncomment to run):
# oldclusters <- data.frame(
#   x = runif(1000, 0, 100),
#   y = runif(1000, 0, 100)
# )
# oldclusters <- st_as_sf(oldclusters, coords = c("x", "y"), crs = 29333)
# oldclusters$cluster <- dbscan::dbscan(st_coordinates(oldclusters), eps = 3, minPts = 3)$cluster |> factor()
# oldclusters$cluster[oldclusters$cluster == 0] <- NA  # Remove noise points
# # Assign a timestamp column from the last month
# oldclusters$timestamp <- Sys.time() - lubridate::days(7) - lubridate::days(sample(1:30, nrow(oldclusters), replace = TRUE))
# 
# newclusters <- data.frame(
#   x = runif(1000, 0, 100),
#   y = runif(1000, 0, 100)
# )
# newclusters <- st_as_sf(newclusters, coords = c("x", "y"), crs = 29333)
# newclusters$cluster <- dbscan::dbscan(st_coordinates(newclusters), eps = 3, minPts = 3)$cluster |>
#   factor()
# newclusters$cluster[newclusters$cluster == 0] <- NA  # Remove noise points
# newclusters$timestamp <- Sys.time() - lubridate::days(sample(1:30, nrow(newclusters), replace = TRUE))

# # Run the matching function
# result <- match_sf_clusters(
#   masterclusters = oldclusters,
#   newclusters = newclusters,
#   cluster_id_col = "cluster",
#   matching_threshold = units::set_units(7, "metres"),
#   matching_criteria = "gmedian",
#   plot_results = TRUE
# )
# result$match_plot
# # ggsave(result$match_plot, file = "testing_match_plot.png", width = 25, height = 10)
# # 
# # 
# # # Save output for testing
# # saveRDS(result, file = "testing_output_data.rds")
# 
# 
# ggplot() +
#   geom_sf(
#     data = result$matched_master_data,
#     aes(color = cluster,
#         shape = "Old"
#     )
#   ) + 
#   geom_sf(
#     data = newclusters,
#     aes(color = cluster,
#         shape = "New"
#     )
# )