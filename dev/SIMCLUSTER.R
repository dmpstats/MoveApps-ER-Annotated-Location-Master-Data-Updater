# SIMULATE BIRDS 
# Here, we'll run a simple version of cluster-updating.
# On each iteration, we'll generate some paths, cluster them,
# and try to merge them with the previous. 

# Setup ---------------------


library(dplyr); library(sf); library(move2); library(cli)

source("match_sf_clusters.R")
source("name_clusters.R")
source("ERsplit.R")

set.seed(123456)

# Iteration ---------------------

# Define parameters
startdate <- lubridate::now() - lubridate::days(31)
numBirds <- 20 # number of birds to simulate
tagFreq <- 1 # number of hours per report # (e.g., 1 hour = 1 report per hour)
numDays <- 20 # number of days over which we should iterate
dataWindow <- 28 # number of days of data we will keep in the call window
sigma <- 0.05

initLocs <- rep(
  list(c(0, 500000)), numBirds
)
historicalData <- mt_sim_brownian_motion(
  # Create 2 months of hourly timestamps leading up to the start date
  t = seq(startdate - lubridate::days(62), 
          startdate, by = "hour"),
  sigma = sigma,
  tracks = paste0("bird", 1:numBirds),
  start_location = initLocs
) |> 
  st_set_crs(32631)

historicalData <- historicalData |> 
  mutate(
    observation_id = stringi::stri_rand_strings(nrow(historicalData), length = 20),
    source_id = stringi::stri_rand_strings(nrow(historicalData), length = 20)
  )

# Get some initial clusters for the historical data
historicClusters <- historicalData |> 
  st_transform(32631) |>
  st_coordinates() |> 
  dbscan::dbscan(
    eps = 0.5,
    minPts = 5
  )
historicalData <- historicalData |> 
  mutate(
    clust_id = historicClusters$cluster,
    clust_id = ifelse(clust_id == 0, NA, clust_id),
    cluster_status = ifelse(
      !is.na(clust_id), "ACTIVE", NA)
  ) |> 
  mutate(
    latitude = st_coordinates(historicalData |> st_transform(4326))[, 2],
    longitude = st_coordinates(historicalData |> st_transform(4326))[, 1]
  ) 

# Get the list of tracks in the correct order
track_order <- unique(historicalData$track)

# Extract last positions, keep track name
finalPositions <- historicalData %>%
  st_transform(32631) %>%
  arrange(track, time) %>%
  group_by(track) %>%
  slice_tail(n = 1) %>%
  ungroup() %>%
  select(track) %>%
  cbind(st_coordinates(.)) %>%
  select(track, X, Y)

# Reorder by track_order
finalPositions <- finalPositions %>%
  mutate(track = factor(track, levels = track_order)) %>%
  arrange(track)

# Create list of coordinates (in correct order)
initLocs <- finalPositions %>%
  data.frame() %>%
  select(X, Y) %>%
  as.matrix() %>%
  split(1:nrow(.)) %>%
  lapply(as.numeric)

# Iterate
for (day in 1:numDays) {
  
  cli_h1("Day {day} of {numDays}")
  TODAY <- startdate + lubridate::days(day)
  
  historicalData <- historicalData |> mutate(Stage = "Historical")
  
  
  cli_inform("Initial locations:")
  cli_ul(initLocs |> 
           lapply(paste, collapse = ", "))
  
  # Simulate data 
  todayData <- mt_sim_brownian_motion(
    t = startdate + lubridate::days(day) + seq(0, 24, by = tagFreq),
    sigma = sigma,
    # tracks = paste0("bird", 1:numBirds),
    tracks = unique(historicalData$track),
    start_location = initLocs
  ) |> 
    st_set_crs(32631)
  
  # However, we want to also stack on the most recent 27 days of data from the 
  # historicalData (which will be duplicated)
  cli_inform("Binding most recent {dataWindow} days of historical data to today's data.")  
  todayData <- todayData |> 
    mt_stack(
      historicalData |> 
        filter(time > (TODAY - days(dataWindow + 1))),
      .track_combine = "merge"
    ) |> 
    mutate(Stage = "New") |> 
    select(-any_of(c("source_id", "observation_id")))
  
  cli_inform("Now operating on {nrow(historicalData)} historical obs and {nrow(todayData)} new obs.")
  
  # Clustering ---------------------
  
  # Run a simple clustering process on the NEW data
  cli_inform("Clustering new data")
  
  clusters <- todayData |> 
    st_transform(32631) |>
    st_coordinates() |> 
    dbscan::dbscan(
      eps = 0.5,
      minPts = 5
    ) 
  todayData <- todayData |> 
    mutate(
      clust_id = clusters$cluster,
      clust_id = ifelse(clust_id == 0, NA, clust_id)
    ) |> 
    mutate(
      longitude = st_coordinates(todayData |> st_transform(4326))[, 1],
      latitude = st_coordinates(todayData |> st_transform(4326))[, 2]
    )
  cli_inform("Found {n_distinct(todayData$clust_id)} clusters in new data.")
  
  
  # PROCESS TESTING -------------------------------
  
  # CC: This is the step that needs to be implemented.
  # match_sf_clusters [identifies clusters that match, and carries on those 
  # without matches] 
  # -> ERsplit [does some pre-ER handling and sorts into POST and PATCH]
  # -> nameClusters [this can be used together or separately on the POST/PATCH datasets 
  # because each cluster has a unique identifier. Could even be moved before ERsplit]
  
  # First, let's match the clusters
  cli::cli_inform("Matching clusters")
  matchclusters <- match_sf_clusters(
    masterclusters = historicalData,
    newclusters = todayData,
    cluster_id_col = "clust_id",
    timestamp_col = "time",
    thresh_days = 7,
    matching_threshold = units::set_units(5, "metres"),
    matching_criteria = "centroid",
    plot_results = TRUE
  )
  cli::cli_alert_success("Clusters matched.")
  Sys.sleep(5)
  
  ggsave(matchclusters$match_plot, 
         file = paste0("testplots/matchplot_day_", day, ".png"),
         width = 15, height = 8,
         create.dir = T
  )
  
  # Add ER-required columns
  matched_master_obs <- matchclusters$matched_master_data # |> 
  # cbind(st_coordinates(matchclusters$matched_master_data)) |>
  # rename(longitude = X, latitude = Y)
  
  
  # Next, let's removing the duplicates and sort into POST and PATCH
  cli_inform("Removing duplicates and sorting into POST and PATCH")
  
  sortclusters <- ERsplit(
    masterdata = matched_master_obs,
    newdata = todayData,
    cluster_id_col = "clust_id",
    time_col = "time",
    animal_id_col = "track",
    active_threshold_days = 14
  )
  

  # Update and iterate 
  outdat <- mt_stack(
    sortclusters$patch_data,
    sortclusters$post_data,
    .track_combine = "merge"
  ) |> 
    nameClusters(
      xcol = "longitude", 
      ycol = "latitude",
      timecol = "time",
      cluster_id_col = "clust_id"
    ) |> 
    arrange(track, time)
  
  # simulate ER requirements
  outdat <- outdat |> 
    mutate(
      observation_id = stringi::stri_rand_strings(nrow(outdat), length = 20),
      source_id = stringi::stri_rand_strings(nrow(outdat), length = 20)
    )
  

  # Move to next step -------------------
  
  cli_inform("Advancing to next day")
  
  # Ensure the outdata becomes historical data
  historicalData <- outdat |> 
    select(all_of(c(
      "time", "track", "geometry", "observation_id", "source_id", "clust_id", "Stage", "cluster_status", "longitude", "latitude"
    ))) 
  
  cli_inform("Historical data now has {nrow(historicalData)} observations and {length(unique(historicalData$clust_id))} clusters.")
  
  cli_inform('Removing {sum(!is.na(historicalData$cluster_status) & historicalData$cluster_status == "CLOSED")} closed-cluster observations'
)
  
  historicalData <- historicalData |> 
    filter(
      is.na(cluster_status) | 
        cluster_status == "ACTIVE"
    )
  
  
  # Extract a list of final positions of each bird
  # finalPositions <- historicalData %>%
  #   cbind(st_coordinates(.)) %>%
  #   group_by(track) %>%
  #   slice_tail(n = 1) %>%
  #   ungroup() %>%
  #   select(track, x = X, y = Y) %>%
  #   data.frame() 
  # # And reformat as list of vectors
  # initLocs <- finalPositions %>%
  #   select(x, y) %>%
  #   as.matrix() %>%
  #   split(1:nrow(.)) %>%
  #   lapply(as.numeric)
  # names(initLocs) <- NULL

  
  # Get the list of tracks in the correct order
  track_order <- unique(historicalData$track)
  
  # Extract last positions, keep track name
  finalPositions <- historicalData %>%
    st_transform(32631) %>%
    arrange(track, time) %>%
    group_by(track) %>%
    slice_tail(n = 1) %>%
    ungroup() %>%
    select(track) %>%
    cbind(st_coordinates(.)) %>%
    select(track, X, Y)
  
  # Reorder by track_order
  finalPositions <- finalPositions %>%
    mutate(track = factor(track, levels = track_order)) %>%
    arrange(track)
  
  # Create list of coordinates (in correct order)
  initLocs <- finalPositions %>%
    data.frame() %>%
    select(X, Y) %>%
    as.matrix() %>%
    split(1:nrow(.)) %>%
    lapply(as.numeric)
  
  cli_inform("Plotting outdata")
  
  # plot this step
  plotData <- historicalData |> 
    group_by(track) |> 
    arrange(time) |> 
    group_by(track, Stage) |> 
    summarise(
      geometry = st_combine(geometry)
    ) |> 
    st_cast("LINESTRING") 
  outplot <- ggplot() +
    geom_sf(data = plotData,
            aes(color = track, linetype = Stage)) +
    theme_bw()
  print(outplot)
  
  # Wait for 5s before next stage
  Sys.sleep(5)
}

