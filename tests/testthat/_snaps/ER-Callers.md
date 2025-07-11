# ra_post_obs() fails if key required columns are missing

    Code
      ra_post_obs(data = slice(test_sets$nam_1, 1:5), tm_id_col = mt_time_column(test_sets$nam_1), api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/", token = er_tokens$standrews.dev$brunoc)
    Condition
      [1m[33mError[39m in `ra_post_obs()`:[22m
      [38;5;254m[33m![38;5;254m `data` is missing the following required columns: "cluster_status" and "tag_id".[39m

# get_obs(): fails when and as expected

    Code
      get_obs(api_base_url = "https://WRONG_URL.co.uk", token = er_tokens$standrews.dev$brunoc)
    Condition
      [1m[33mError[39m in `httr2::req_perform()`:[22m
      [33m![39m Failed to perform HTTP request.
      [1mCaused by error in `curl::curl_fetch_memory()`:[22m
      [33m![39m Could not resolve hostname [WRONG_URL.co.uk]: Could not resolve host: WRONG_URL.co.uk

---

    Code
      get_obs(api_base_url = "https://standrews.dev.pamdas.org/api/v1.0/", token = "INVALID_TOKEN")
    Condition
      [1m[33mError[39m in `httr2::req_perform()`:[22m
      [33m![39m HTTP 401 Unauthorized.
      • OAuth error: invalid_token - The access token is invalid.
      • realm: api
      • Failed to request observations historical data.

---

    Code
      get_obs(min_date = "INVALID DATE")
    Condition
      [1m[33mError[39m in `get_obs()`:[22m
      [38;5;254m[33m![38;5;254m `min_date` must be <POSIXt>[39m

---

    Code
      get_obs(max_date = "INVALID DATE")
    Condition
      [1m[33mError[39m in `get_obs()`:[22m
      [38;5;254m[33m![38;5;254m `max_date` must be <POSIXt>[39m

---

    Code
      get_obs(created_after = "INVALID DATE")
    Condition
      [1m[33mError[39m in `get_obs()`:[22m
      [38;5;254m[33m![38;5;254m `created_after` must be <POSIXt>[39m

---

    Code
      get_obs(filter = "INVALID_FILTER")
    Condition
      [1m[33mError[39m in `get_obs()`:[22m
      [38;5;254m[33m![38;5;254m `filter` must be numeric[39m

---

    Code
      get_obs(filter = factor(2))
    Condition
      [1m[33mError[39m in `get_obs()`:[22m
      [38;5;254m[33m![38;5;254m `filter` must be numeric[39m

