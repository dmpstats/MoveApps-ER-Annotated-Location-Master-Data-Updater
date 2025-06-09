library(httr2)
library(dplyr)
library(tidyr)
library(jsonify)
library(cli)
`%!in%` <- Negate(`%in%`)


# Function for patching observations with new data using PATCH
ER_PATCH_OBS <- function(api_url, 
                         token, 
                         observations, 
                         additional_cols = NULL # User-defined additional attributes
) {
  
  ## Step 1: Input Validation -------------------------------------------------
  if ("id" %!in% names(observations)) {
    stop("The input data must contain an 'id' column to match observations to existing records.")
  }
  
  if (!is.null(additional_cols) && any(additional_cols %!in% names(observations))) {
    stop(paste0(
      "Specified additional columns ", paste(additional_cols[additional_cols %!in% names(observations)], collapse = ", "),
      " are not present in the input data. Please ensure these columns exist."
    ))
  }
  
  ## Step 2: Append API Endpoint -------------------------------------------------
  # Ensure the base URL ends without a trailing slash
  api_url <- sub("/$", "", api_url)
  api_url <- paste0(api_url, "/api/v1.0/observation/")
  
  ## Step 3: Pre-Processing ---------------------------------------------------
  # Convert observations to JSON-compatible format
  observations_json_ready <- observations %>%
    # Convert latitude and longitude to strings
    mutate(
      latitude = as.character(latitude),
      longitude = as.character(longitude)
    ) %>%
    tidyr::nest(
      location = c(latitude, longitude),
      additional = any_of(additional_cols), # Include user-defined columns in 'additional'
      .by = c(id, recorded_at, source)
    ) %>%
    mutate(
      location = lapply(location, as.list),
      additional = lapply(additional, as.list) # Ensure 'additional' is JSON-serializable
    )
  
  ## Step 4: Perform PATCH Requests -------------------------------------------
  cli::cli_inform("Starting PATCH requests for observation updates...")
  
  results <- observations_json_ready %>%
    split(~id) %>%
    purrr::map_int(function(observation) {
      # Extract the observation ID
      observation_id <- observation$id[1]
      
      # Convert observation to JSON
      observation_json <- observation %>%
        select(-id) %>% # Remove the 'id' column as it's part of the URL path
        as.list() %>%
        jsonify::to_json(unbox = TRUE) # Serialize to JSON (no need for `toString()`)
      
      cli::cli_inform("Showing request...")
      
      print(observation_json)
      
      # Prepare PATCH request
      cli::cli_inform("Performing request to {paste0(api_url, observation_id)}")
      req <- request(paste0(api_url, observation_id)) %>%
        req_method("PATCH") %>%  # Explicitly set the HTTP method to PATCH
        req_headers(
          "Authorization" = paste("Bearer", token),
          "Accept" = "application/json",
          "Content-Type" = "application/json"
        ) %>%
        req_body_raw(observation_json)
      
      # Send request
      res <- req_perform(req)
      
      # Check for errors
      if (resp_is_error(res)) {
        cli::cli_alert_danger("Error in PATCH request for ID {observation_id}: ", resp_body_string(res))
        return(resp_status(res))
      } else {
        cli::cli_alert_success("Observation ID {observation_id} updated successfully.")
        return(resp_status(res))
      }
    })
  
  ## Step 5: Log Results ------------------------------------------------------
  successful_requests <- sum(results == 200)
  total_requests <- length(results)
  cli::cli_inform(glue::glue("{successful_requests} of {total_requests} observations updated successfully."))
  
  return(results)
}
