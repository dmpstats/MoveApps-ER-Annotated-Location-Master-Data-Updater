ER_GET <- function(server,
                   token,
                   attribute = c("observations", "sources"),
                   min_date = NULL,
                   max_date = NULL,
                   include_details = FALSE,
                   page_size = 5000) {
  
  # If min_date is not provided, use 31 days from sys.time
  if (is.null(min_date)) {
    min_date <- Sys.time() - lubridate::days(31)
  }
  # If max_date is not provided, use sys.time
  if (is.null(max_date)) {
    max_date <- Sys.time()
  }
  
  # Format dates as ISO 8601 strings (required by the API)
  min_date <- format(min_date, "%Y-%m-%dT%H:%M:%SZ")
  max_date <- format(max_date, "%Y-%m-%dT%H:%M:%SZ")
  
  # Prepare the base URL ---------------------------------------
  base_url <- paste0(server, "/api/v1.0/", attribute, "/")
  
  # Initialize variables for pagination
  all_results <- list()
  next_page_url <- base_url
  
  # Loop to retrieve all pages
  while (!is.null(next_page_url)) {
    # Prepare the request with additional query parameters
    req <- httr2::request(next_page_url) |>
      httr2::req_url_query(
        since = min_date,
        until = max_date,
        page_size = page_size,
        include_details = tolower(as.character(include_details)) # Convert logical to string
      ) |>
      httr2::req_auth_bearer_token(token) |> 
      httr2::req_headers(
        "accept" = "application/json"#,
        #"Authorization" = paste0('Bearer ', token)
      )
    
    # Perform the request
    res <- req |> httr2::req_perform()
    
    # Parse the response
    if (httr2::resp_status(res) == 200) {
      data <- httr2::resp_body_json(res, simplifyVector = TRUE)
      cli::cli_alert_success("{attribute} retrieved successfully (Page).")
      
      # If location is non-null, unnest the column
      if (!is.null(data$data$results$location)) {
        data$data$results <- data$data$results |>
          tidyr::unnest(location, names_sep = "_") |> 
          dplyr::rename(
            latitude = "location_latitude",
            longitude = "location_longitude"
          )
      }
      
      # observation_details contains a nested list equal in 
      # length to the data. We want to first turn this into a nested
      # column, and then unnest:
      if (!is.null(data$data$results$observation_details)) {
        
        if (class(data$data$results$observation_details) == "data.frame") {
          observation_details_out <- data$data$results$observation_details
        } else {
          observation_details_out <- data$data$results$observation_details |>
            lapply(data.frame) |> 
            # If any entry is a 0-row data frame or empty list, replace it 
            # with a data.frame containing only PLACEHOLDER = NA
            #purrr::map(~ if (nrow(.x) == 0 || is.null(.x)) data.frame(PLACEHOLDER = NA) else .x) |>
            purrr::map( \(x){ if (nrow(x) == 0 || is.null(x)) data.frame(PLACEHOLDER = NA) else x}) |> 
            dplyr::bind_rows() |> 
            dplyr::select(-dplyr::any_of("PLACEHOLDER")) # this was just for merging
        }
    
        # Validate that nrow(observation_details_out) matches nrow(data$data$results)
        if (nrow(observation_details_out) != nrow(data$data$results)) {
          browser()
          cli::cli_abort(
            "Observation details length mismatch: {nrow(observation_details_out)} vs {nrow(data$data$results)}"
          )
        }
      
      # Bind observation_details_df back to the main data frame
      data$data$results <- data$data$results |>
        # Drop the main observation_details col
        dplyr::select(-"observation_details") |>
        dplyr::bind_cols(observation_details_out)
    }
      # Append the current page of results to the list
      all_results[[next_page_url]] <- data$data$results

      
      # Update the next page URL for pagination
      next_page_url <- data$data[["next"]]
      
    } else {
      cli::cli_alert_danger(
        "Failed to retrieve {attribute}. Status code: ", 
        httr2::resp_status(res)
      )
      cli::cli_abort(httr2::resp_body_string(res))
      return(NULL)
    }
  }
  
  # Combine all results into a data frame
  if (length(all_results) > 0) {
    combined_results <- do.call(bind_rows, lapply(all_results, as.data.frame))
  } else {
    combined_results <- data.frame()
  }
  
  return(combined_results)
}