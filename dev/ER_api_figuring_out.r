library(httr2)
library(jsonlite)
library(jsonify)



req <- httr2::request('https://sandbox.pamdas.org/api/v1.0/status/')

req <- httr2::request('https://standrews.pamdas.org/api/v1.0/status/')
resp <- req_perform(req)
resp
resp_status(resp)

resp |> resp_content_type()
resp_json <- resp |> resp_body_json()
str(resp_json)




# Animals (i.e. tracks) ------------------------------------------

subjects_request <- request('https://standrews.pamdas.org/api/v1.0/subjects/') |> 
  req_headers(
    "accept" = "application/json",
    #"X-CSRFToken" = "TbnJwVtphDG9GmYiM769D6ryiSNbU8hfvto28BrBMaPfeeT5PtUceXrZqMQehmep",
    "Authorization" = 'Bearer 5vSdiVs7lrQL8wMfKoiYPo272MNczKBARC'
  )

subjects_resp <- subjects_request |> req_perform()|> resp_body_json()


# Animal Tracks (i.e location events) ------------------------------------------
example_subject <- subjects_resp$data[[1]]$id

track_request <- request(paste0('https://standrews.pamdas.org/api/v1.0/subject/', example_subject, '/tracks/')) |> 
  req_headers(
    "accept" = "application/json",
    #"X-CSRFToken" = "TbnJwVtphDG9GmYiM769D6ryiSNbU8hfvto28BrBMaPfeeT5PtUceXrZqMQehmep",
    "Authorization" = 'Bearer 5vSdiVs7lrQL8wMfKoiYPo272MNczKBARC'
  )


track_resp <- track_request |> req_perform() |> resp_body_json()
str(track_resp$data)

track_request |> req_perform() |> resp_body_string() |> from_json()


# Events (i.e. ???)------------------------------------------

events_request <- request('https://standrews.pamdas.org/api/v1.0/activity/events/?page_size=100') |> 
  req_headers(
    "accept" = "application/json",
    #"X-CSRFToken" = "TbnJwVtphDG9GmYiM769D6ryiSNbU8hfvto28BrBMaPfeeT5PtUceXrZqMQehmep",
    "Authorization" = 'Bearer 5vSdiVs7lrQL8wMfKoiYPo272MNczKBARC'
  )

events_resp <- events_request |> req_perform()|> resp_body_json()

events_resp$data$results[[3]]$title
events_resp$data$results[[3]]$updated_at

events_resp$data$results[[25]]$title
events_resp$data$results[[25]]$updated_at

length(events_resp$data$results)


#userid <- "703e423b-47e8-4d3f-b6ef-deb4c1c4e44e"
