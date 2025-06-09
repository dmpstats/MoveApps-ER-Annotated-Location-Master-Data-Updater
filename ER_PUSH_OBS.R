library(httr2)
library(dplyr)
library(tidyr)
library(jsonify)
library(cli)
`%!in%` <- Negate(`%in%`)

# The below helper function is for posting observations to ER.
# The postal data must be a data.frame containing the following columns: 
# latitude, longitude, recorded_at, source.
# Source must match a valid UUID within the ER server: if not, this will fail.

ER_PUSH_OBS <- function(api_url, 
                        token, 
                        observations, 
                        batch_max_mb = 10, 
                        grouping_col = NULL,
                        additional_cols = NULL # User-defined additional attributes
) {
  
  ## Step 1: Input Validation -------------------------------------------------
  if (!is.null(grouping_col) && grouping_col %!in% names(observations)) {
    stop(paste0(
      "Specified column name '", grouping_col, "' must be present in the input data. ",
      "Please ensure the grouping column for batch uploading is correctly specified."
    ))
  }
  if (!is.null(additional_cols) && any(additional_cols %!in% names(observations))) {
    stop(paste0(
      "Specified additional columns ", paste(additional_cols[additional_cols %!in% names(observations)], collapse = ", "),
      " are not present in the input data. Please ensure these columns exist."
    ))
  }
  
  ## Step 2: Append API Endpoint -------------------------------------------------
  # Ensure the base URL ends without a trailing slash to avoid duplication when appending the endpoint
  api_url <- sub("/$", "", api_url)
  api_url <- paste0(api_url, "/api/v1.0/observations/")
  
  ## Step 3: Pre-Processing ---------------------------------------------------
  # Add explicit grouping column, if not provided
  if (is.null(grouping_col)) {
    observations$group_id <- 1:nrow(observations) # Treat each row as independent
  } else {
    observations$group_id <- observations[[grouping_col]]
  }
  
  # Add a row index for batch nesting
  observations <- observations %>%
    mutate(rowindex = row_number())
  
  ## Step 4: Convert Observations to JSON-Compatible Format -------------------
  observations_json_ready <- observations %>%
    tidyr::nest(
      location = c(latitude, longitude),
      additional = any_of(additional_cols), # Include user-defined columns in 'additional'
      .by = c(rowindex, group_id, recorded_at, source)
    ) %>%
    mutate(
      location = lapply(location, as.list),
      additional = lapply(additional, as.list) # Ensure 'additional' is JSON-serializable
    )
  
  ## Step 5: Calculate Batch Sizes --------------------------------------------
  # Estimate the size of a single row in bytes
  sample_row <- observations_json_ready %>%
    slice(1) %>%
    jsonify::to_json(unbox = TRUE) %>%
    toString()
  row_json_bytes <- as.numeric(object.size(sample_row))
  
  # Calculate group sizes and assign batch IDs
  batching_tbl <- observations %>%
    count(group_id) %>%
    mutate(
      group_mb = (n * row_json_bytes) / 1024^2, # Estimate group size in MB
      cum_mb = cumsum(group_mb),               # Cumulative size
      batch_id = dense_rank(cum_mb %/% batch_max_mb) # Assign batch IDs
    )
  
  # Merge batch IDs back into the observations
  observations_json_ready <- observations_json_ready %>%
    left_join(batching_tbl %>% select(group_id, batch_id), by = "group_id")
  
  ## Step 6: Perform Batch POST Requests --------------------------------------
  cli::cli_inform("Starting batch upload...")
  
  results <- observations_json_ready %>%
    split(~batch_id) %>%
    purrr::map_int(function(batch_data) {
      # Convert batch to JSON
      batch_json <- batch_data %>%
        jsonify::to_json(unbox = TRUE) %>%
        toString()
      
      # Prepare POST request
      req <- request(api_url) %>%
        req_headers(
          "Authorization" = paste("Bearer", token),
          "Accept" = "application/json",
          "Content-Type" = "application/json"
        ) %>%
        req_body_raw(batch_json)
      
      # Send request
      res <- req_perform(req)
      
      # Check for errors
      if (resp_is_error(res)) {
        cli::cli_alert_danger("Error in POST request: ", resp_body_string(res))
        return(resp_status(res))
      } else {
        cli::cli_alert_success("Batch posted successfully.")
        return(resp_status(res))
      }
    })
  
  ## Step 7: Log Results ------------------------------------------------------
  successful_batches <- sum(results == 201)
  total_batches <- length(results)
  cli::cli_inform(glue::glue("{successful_batches} of {total_batches} batches posted successfully."))
  
  return(results)
}
