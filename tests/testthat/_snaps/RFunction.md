# Input validation works as expected

    Code
      rFunction(data = test_sets$nam_1)
    Output
      [INFO] Checking inputs
    Condition
      [1m[33mError[39m in `rFunction()`:[22m
      [38;5;254m[33m![38;5;254m Argument `api_hostname` is missing.
      `api_hostname` must be an <string>, not `NULL`.[39m

---

    Code
      rFunction(data = test_sets$nam_1, api_hostname = "bla.co.uk")
    Output
      [INFO] Checking inputs
    Condition
      [1m[33mError[39m in `rFunction()`:[22m
      [38;5;254m[33m![38;5;254m Argument `api_token` is missing.
      `api_token` must be an <string>, not `NULL`.[39m

---

    Code
      rFunction(data = test_sets$nam_1, api_hostname = "bla.co.uk", api_token = "XYZ", cluster_id_col = NULL)
    Output
      [INFO] Checking inputs
      [FATAL] `cluster_id_col` is missing.
    Condition
      [1m[33mError[39m in `rFunction()`:[22m
      [38;5;254m[33m![38;5;254m Parameter 'Cluster ID Column' (`cluster_id_col`) must be a string, not `NULL`.
      [36mℹ[38;5;254m Please provide a valid column name for this parameter.[39m

---

    Code
      rFunction(data = test_sets$nam_1, api_hostname = "bla.co.uk", api_token = "XYZ", cluster_id_col = "ABSENT_COLUMN")
    Output
      [INFO] Checking inputs
      [FATAL] Input data does not have column 'ABSENT_COLUMN'. Please provide a valid column name with cluster ID annotations.
    Condition
      [1m[33mError[39m in `rFunction()`:[22m
      [38;5;254m[33m![38;5;254m Specified column name `ABSENT_COLUMN` must be present in the input data.
      [33m![38;5;254m Please provide a valid column name for parameter 'Cluster ID Column' (`cluster_id_col`).
      [36mℹ[38;5;254m Use clustering Apps such as Avian Cluster Detection (<https://www.moveapps.org/apps/browser/81f41b8f-0403-4e9f-bc48-5a064e1060a2>) earlier in the workflow to generate the required column.[39m

---

    Code
      rFunction(data = test_sets$nam_1, api_hostname = "bla.co.uk", api_token = "XYZ", cluster_id_col = "clust_id", lookback = 1.2)
    Output
      [INFO] Checking inputs
    Condition
      [1m[33mError[39m in `rFunction()`:[22m
      [38;5;254m[33m![38;5;254m `lookback` must be an <integer>.[39m

---

    Code
      rFunction(data = dt, api_hostname = "bla.co.uk", api_token = "XYZ")
    Output
      [INFO] Checking inputs
      [INFO] Finished check on required columns in input data - all good!
      [FATAL] Found missing `individual_local_identified` for at least one of the tracks in the input dataset.
    Condition
      [1m[33mError[39m in `rFunction()`:[22m
      [38;5;254m[33m![38;5;254m Each track in input dataset must have a non-missing `individual_local_identifier` attribute.
      [31m✖[38;5;254m The following tracks have missing values for `individual_local_identifier`:
      [36m•[38;5;254m "GA_5404_mocky"
      [36m•[38;5;254m "TO_6485_mocky"
      [36mℹ[38;5;254m Please contact the data manager or person responsible for the tag deployment on Movebank to populate this attribute.[39m

