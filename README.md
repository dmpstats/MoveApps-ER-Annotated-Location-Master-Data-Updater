

# MoveApps-EarthRanger Annotated Location Master Data Updater

MoveApps

Github repository:
<https://github.com/dmpstats/MoveApps-ER-Annotated-Location-Master-Data-Updater>

## Description

This App facilitates the persistent storage and updating of annotated
movement data within a master dataset hosted on an
[EarthRanger](https://www.earthranger.com/) server. It is designed for
workflows where MoveBank data has been processed - such as cleaned,
standardized, or enriched with additional attributes like behavioral
classifications or spatial cluster membership - and needs to be reliably
stored and maintained over time.

It is especially useful in scheduled workflows, where recent outputs
from upstream Apps must be merged with existing historical records
before being pushed to EarthRanger (ER) as part of a centralized data
pipeline.

## Documentation

This App provides a MoveApps–EarthRanger integration for the **permanent
storage and updating** of movement data that has been processed and
annotated with features of interest - such as behavioural states or
spatial cluster memberships - by upstream Apps. It is designed for use
in scheduled workflows, where newly processed data needs to be merged
with historical records and reliably stored in EarthRanger (ER).

The current version is specifically focused on **cluster-level
updating** - ensuring that the status of detected spatial clusters
(`"ACTIVE"` or `"CLOSED"`) is correctly recognised, and that each
associated location event is appropriately updated. Future versions may
extend support for broader data-merging tasks with fewer constraints
around cluster handling.

The App performs three core tasks:

1.  **Retrieve Historical Records**  
    It fetches relevant subject-level movement data from ER based on the
    current input, including:

    - All historical observations associated with **active clusters**.
    - **Unclustered** observations within a configurable `lookback`
      window (i.e., a number of days before the earliest timestamp in
      the input data).

2.  **Merge Datasets**  
    The newest upstream data is combined with historical observations,
    with an emphasis on correctly updating cluster membership
    information (see further details on [cluster
    merging](#cluster-merging-and-updating) below).

3.  **Push to EarthRanger**  
    The resulting dataset - containing both new and updated records - is
    uploaded back to ER for permanent storage.

Currently the App outputs an extended version of the input dataset,
appending all location events associated with **currently ACTIVE
clusters** (as well as in-between non-clustered locations). These
annotated, up-to-date subject-level location records, are intended for
use in downstream Apps that perform cluster-based analysis, such as the
[Avian Cluster
Detection](https://www.moveapps.org/apps/browser/81f41b8f-0403-4e9f-bc48-5a064e1060a2)
and the [Cluster Importance
Scoring](https://www.moveapps.org/apps/browser/e8f5b376-0858-4206-9861-e2cd5fcc8c41)
Apps.

### Cluster merging and updating

A large section of this MoveApp is committed to merging clusters stored
on the EarthRanger with ‘new’ clusters identified through the MoveApps
workflow.

The merging process is spatiotemporal. First, a comparison of all
available clusters identifies all combinations of clusters that are
within: A) `dist_thresh` of one another’s centroids or geometric medians
B) Occuring within `days_thresh` of one another’s timestamps
i.e. fulfilling both the spatial- and temporal- criteria.

In the event of a two-to-one match (two clusters merging/splitting into
one), each individual observation is assigned to the spatially-nearest
cluster centroid.

‘Ongoing’ clusters (i.e. clusters that pre-existed in the EarthRanger
dataset) retain their existing UUID; ‘new’ clusters are assigned a
newly-generated UUID.

### Posting to EarthRanger

We expect there to be a large number of duplicate observations between
both datasets: for example, an observation might exist on the
EarthRanger server, but then be clustered once more by the MoveApps
workflow. In this case, we retain the ‘updated’ data associated with
this observation, but continue to associate it with the same observation
ID on EarthRanger.

This is performed by splitting the data into `POST` data,
i.e. observations that do not yet exist on EarthRanger and need to be
posted, and `PATCH`, data, i.e. observations that *already* exist on
EarthRanger and only need some associated attributes to be updated.

Any cluster whose final timestamp was over `active_days_thresh` days
BEFORE the most recent observation timestamp is marked as `"CLOSED"`.
This means that future runs of this MoveApp will no longer call
observations associated with this cluster: it is assumed to be a
concluded event, and data associated with it will no longer be replaced
by updated attributes. This is implemented to prevent the APIs from
calling excessive volumes of data on each iteration.

### Application scope

#### Generality of App usability

This App was originally developed using vulture movement data annotated
with cluster memberships and behavioural categories. However, it can
likely be applied to other taxonomic groups, provided the data follows a
similar annotation structure.

#### Required data properties

At present, the App is only applicable to location datasets that have
been pre-annotated with cluster membership information. Datasets lacking
this structure will not be compatible with the App’s current update
logic.

### Input type

A `move2::move2_loc` object.

### Output type

A `move2::move2_loc` object.

### Artefacts

None.

### Settings

**EarthRanger API Server** (`api_hostname`): Hostname of the EarthRanger
API server (e.g. ‘sandbox.pamdas.org’).

**EarthRanger API Key** (`api_token`): A valid and active access token
for the selected EarthRanger API Server.

**Cluster ID Column** (`cluster_id_col`): Name of the column in the
input data that contains cluster IDs associated with location points.

**Lookback (days)** (`lookback`): Number of days to look back from the
earliest timestamp in the input data to retrieve unclustered historical
observations for updating. Note: Observations assigned to active
clusters are always retrieved automatically.

**Attributes to store in EarthRanger** (`store_cols`): Comma- or
semicolon-separated list of attribute names to store along each
observation’s location and timestamp in EarthRanger. If `NULL`, all
attributes in input data are stored.

**Merging Time Threshold (days)** (`days_thresh`): Maximum number of
days within which clusters are considered for merging. Clusters that
occur within this time frame can be merged together.

**Merging Distance Threshold (meters)** (`dist_thresh`): Maximum
distance between cluster centroids or geometric medians for clusters to
be considered for merging. Clusters within this distance can be merged
together.

**Spatial-Matching Criteria** (`match_criteria`): Whether clusters
should be merged by a comparison of their centroids `centroid` or their
geometric medians `gmedian`. The latter is more robust to outliers.

**Closure Threshold (days)** (`active_days_thresh`): Number of days
after the most recent observation timestamp after which a cluster is
considered closed. Clusters whose final timestamp is older than this
threshold will not be included in future updates.

### Changes in output data

This App plays a central role in updating a master dataset stored in an
external system (EarthRanger), while also preparing data for downstream
workflow Apps. As such, it applies significant transformations to the
input data. Key changes include:

- The input data is expanded to include all historical location records
  linked to currently ACTIVE clusters, as well as in-between
  non-clustered locations for tracks/subjects associated with those
  clusters (all retrieved from ER via it’s API).

- Aside from `track_id_column`, `time-column` and the geometry
  `sf-column`, only those columns specified under parameter **Attributes
  to store in EarthRanger** (`store_cols`) are retained in the output.

- The column defined as `track_id_column` in the input is renamed to
  `"track_id"` in the output. This aims to ensure consistency across
  scheduled runs and alignment with data stored in ER, even if the input
  column name varies over time.

- The output’s track-level table is reduced to the following attributes:
  “`track_id`”, “`tag_id`”, “`individual_local_identifier`”,
  “`deployment_id`”, “`individual_id`” & “`study_id`”. (This list may be
  extended in future versions.)

- Two additional columns are added to the output `<move2>` event table:

  - `cluster_uuid` - the unique identifier of each cluster, as stored in
    the ER master dataset.
  - `cluster_state`- the cluster’s status at the time of the current
    run, either `"ACTIVE"` or `"CLOSED"`.

### Most common errors

The App will stop and return an error under any of the following
conditions:

- Invalid API server: The hostname provided under **EarthRanger API
  Server** is missing, incorrect, or unreachable. Make sure the server
  address is correct and that you have access of the specified
  EarthRanger instance.

- Invalid API key: The value passed to **EarthRanger API Key** is
  missing or invalid. Ensure you provide a valid access token linked to
  an EarthRanger account with sufficient permissions to read and write
  data.

- The **Cluster ID Column** is missing (i.e. `NULL`) or the specified
  value does not exist in the input dataset. Double-check that the
  column name matches exactly (including case sensitivity) one of the
  columns in the incoming data.

- Errors may occur due to brief network interruptions or issues with the
  MoveApps or ER hosting servers. However, such disruptions are expected
  to be infrequent and short-lived, as both platforms are reliable and
  well-maintained systems.

### Null or error handling

- **Attributes to Store in EarthRanger**: If set to `NULL` (the
  default), all attributes present in the input data will be passed to
  EarthRanger and stored in the master dataset.

- **EarthRanger API Server** and **EarthRanger API Key**: These
  parameters are required. If left empty (NULL, the default), the App
  will throw an error. No default values are provided due to access
  restrictions and security considerations.
