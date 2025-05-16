library('move2')
library('lubridate')

## The parameter "data" is reserved for the data object passed on from the previous app

# to display messages to the user in the log file of the App in MoveApps
# one can use the function from the logger.R file:
# logger.fatal(), logger.error(), logger.warn(), logger.info(), logger.debug(), logger.trace()

# Showcase injecting app setting (parameter `year`)
rFunction = function(data, where) {
  
  logger.info(paste0("Welcome to ", where, "!!!"))
  
  
  data <- data |> 
    dplyr::mutate(
      mock_master_clust_id = sort(sample(LETTERS[1:5], dplyr::n(), replace = TRUE))
    )
  
  
  # task1
  hist <- fetch_hist_data(time_filter, spatial_filter, status_filter)
  
  
  # task 2
  new_data <- merge_clusters(data, hist)
  
  
  # task 3
  send_updated_data(new_data)
  
  
  return(new_data)
  
   
  # if (!is.null(result)) {
  #   # Showcase creating an app artifact. 
  #   # This artifact can be downloaded by the workflow user on Moveapps.
  #   artifact <- appArtifactPath("plot.png")
  #   logger.info(paste("plotting to artifact:", artifact))
  #   png(artifact)
  #   plot(result[mt_track_id_column(result)], max.plot=1)
  #   dev.off()
  # } else {
  #   logger.warn("nothing to plot")
  # }
  # # Showcase to access a file ('auxiliary files') that is 
  # # a) provided by the app-developer and 
  # # b) can be overridden by the workflow user.
  # fileName <- getAuxiliaryFilePath("auxiliary-file-a")
  # logger.info(readChar(fileName, file.info(fileName)$size))
  # 
  # # provide my result to the next app in the MoveApps workflow
  # return(result)
  
}



fetch_hist_data <- function(time_filter = NULL, 
                            spatial_filter = NULL, 
                            status_filter = NULL){
  
  logger.info("Fetching data from ER...")
  
  move2::mt_sim_brownian_motion(t = seq(now() - days(1), now(), by = "1 hour")) |> 
    sf::st_set_crs(4326)
  
}


merge_clusters <- function(data, hist){
  
  logger.info("Merrrrrrging data...")
  
  move2::mt_stack(data, hist)
  
}


send_updated_data <- function(data){
  logger.info("Whoop - updated data sent to ER with noooooooo issues!")
}

