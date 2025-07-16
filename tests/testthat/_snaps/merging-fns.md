# match_sf_clusters() fails with invalid inputs

    Code
      match_sf_clusters(hist_dt = data.frame(), new_dt = slice(test_sets$nam_1, 30:
      100))
    Output
      [INFO] Matching Spatial Clusters
      [WARN]    |- Retrieved historical data is empty; skipping matching with newest data.
    Condition
      Error in `match_sf_clusters()`:
      ! argument "cluster_id_col" is missing, with no default

---

    Code
      match_sf_clusters(hist_dt = slice(test_sets$nam_1, 1:10), new_dt = data.frame(),
      )
    Output
      [INFO] Matching Spatial Clusters
    Condition
      Error in `match_sf_clusters()`:
      ! `new_dt` must be a <move2> object, not <data.frame>.

---

    Code
      match_sf_clusters(hist_dt = slice(test_sets$nam_1, 1:10), new_dt = slice(
        test_sets$nam_1, 30:100), cluster_id_col = "NONEXISTENT_COLUMN")
    Output
      [INFO] Matching Spatial Clusters
    Condition
      Error in `match_sf_clusters()`:
      ! Column "NONEXISTENT_COLUMN" must be present in `new_dt`.

---

    Code
      match_sf_clusters(hist_dt = slice(test_sets$nam_1, 1:10), new_dt = slice(
        test_sets$nam_1, 30:100), cluster_id_col = "clust_id", timestamp_col = "NONEXISTENT_COLUMN")
    Output
      [INFO] Matching Spatial Clusters
    Condition
      Error in `match_sf_clusters()`:
      ! Column "NONEXISTENT_COLUMN" must be present in `new_dt`.

---

    Code
      match_sf_clusters(hist_dt = hist, new_dt = slice(test_sets$nam_1, 30:100),
      cluster_id_col = "clust_id", timestamp_col = "timestamp")
    Output
      [INFO] Matching Spatial Clusters
    Condition
      Error in `match_sf_clusters()`:
      ! Column "cluster_uuid" must be present in `hist_dt`.

---

    Code
      match_sf_clusters(hist_dt = hist, new_dt = slice(test_sets$nam_1, 30:100),
      cluster_id_col = "clust_id", timestamp_col = "timestamp")
    Output
      [INFO] Matching Spatial Clusters
    Condition
      Error in `match_sf_clusters()`:
      ! Column "recorded_at" must be present in `hist_dt`.

---

    Code
      match_sf_clusters(hist_dt = hist, new_dt = slice(test_sets$nam_1, 30:100),
      cluster_id_col = "clust_id", timestamp_col = "timestamp", dist_thresh = 4)
    Output
      [INFO] Matching Spatial Clusters
    Condition
      Error in `match_sf_clusters()`:
      ! `dist_thresh` must be a <units> object.

---

    Code
      match_sf_clusters(hist_dt = hist, new_dt = slice(test_sets$nam_1, 30:100),
      cluster_id_col = "clust_id", timestamp_col = "timestamp", match_criteria = "INVALID_CHOICE")
    Output
      [INFO] Matching Spatial Clusters
    Condition
      Error in `match_sf_clusters()`:
      ! `match_criteria` must be one of "gmedian" or "centroid", not "INVALID_CHOICE".

# match_sf_clusters() works as expected

    Code
      out$match_table
    Output
      NULL

---

    Code
      out$matched_master_data
    Output
      NULL

---

    Code
      out$matched_master_data
    Output
      NULL

---

    Code
      out$match_table
    Output
      NULL

---

    Code
      out$matched_master_data
    Output
      NULL

---

    Code
      out$match_table
    Output
      NULL

---

    Code
      out$matched_master_data
    Output
      NULL

---

    Code
      out$match_table
    Output
      NULL

