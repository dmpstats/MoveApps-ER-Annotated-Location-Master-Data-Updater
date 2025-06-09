library(httr2)
library(purrr)
library(cli)
library(glue)

# Function to delete observations by a list of IDs
ER_DELETE_OBS <- function(api_url, token, observation_ids) {
  # Input validation
  if (is.null(observation_ids) || length(observation_ids) == 0) {
    stop("No observation IDs provided. Please provide a non-empty list of IDs to delete.")
  }
  
  # Ensure the API URL ends with a slash
  if (!grepl("/$", api_url)) {
    api_url <- paste0(api_url, "/")
  }
  
  # Iterate over the list of IDs to delete observations
  results <- map(observation_ids, function(id) {
    # Construct the DELETE URL for the observation
    delete_url <- paste0(api_url, "observation/", id, "/")
    
    # Prepare the DELETE request
    req <- request(delete_url) %>%
      req_headers(
        "Authorization" = paste("Bearer", token),
        "Accept" = "*/*"
      )
    
    # Perform the request
    res <- req_perform(req)
    
    # Check the response
    if (resp_status(res) %in% c(200, 204)) {
      cli::cli_alert_success(glue("Observation ID {id} deleted successfully."))
      return(list(id = id, status = "success"))
    } else {
      cli::cli_alert_danger(glue("Failed to delete Observation ID {id}. Status code: {resp_status(res)}"))
      cli::cli_alert_info(glue("Response message: {resp_body_string(res)}"))
      return(list(id = id, status = "failed", response = resp_body_string(res)))
    }
  })
  
  # Return the deletion results
  return(results)
}
