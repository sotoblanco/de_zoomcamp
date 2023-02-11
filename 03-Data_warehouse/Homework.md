# Week 3 Homework

**Important Note:**

You can load the data however you would like, but keep the files in .GZ Format. If you are using orchestration such as Airflow or Prefect do not load the data into Big Query using the orchestrator.
Stop with loading the files into a bucket.

NOTE: You can use the CSV option for the GZ files when creating an External Table

SETUP:
Create an external table using the fhv 2019 data.
Create a table in BQ using the fhv 2019 data (do not partition or cluster this table).
Data can be found here: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv

## Question 1:
What is the count for fhv vehicle records for year 2019?

- 65,623,481
- 43,244,696
- 22,978,333
- 13,942,414

Proposed solution:

Step 1: Download the data from the website and upload it to a bucket in GCP

We use the ``q1_web_to_gcs.py`` file to download the data from the website and upload it to a bucket in GCP.

Step 2: Create a table in BigQuery using one of the files in the bucket to get the schema (remove the first set of data)

Step 3: Upload the data to BigQuery using ``q1_gcs_to_bq.py``

Step 4: Run the query to get the count
```sql
SELECT COUNT(*) FROM `dtc-de-course-374821.fhv.tripdata_csv`;
```

## Question 2:

Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 25.2 MB for the External Table and 100.87MB for the BQ Table
- 225.82 MB for the External Table and 47.60MB for the BQ Table
- 0 MB for the External Table and 0MB for the BQ Table
- 0 MB for the External Table and 317.94MB for the BQ Table

Proposed solution:

Step 1: In BigQuery Create a external table:

```sql
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-374821.fhv.external_fhv2`
OPTIONS (
  format = "CSV",
  uris = ['gs://zoomcampde-bucket/data/fhv/fhv_tripdata_2019-*.csv.gz']
);
```

Step 2: Select the Query of the external table to get the count of distinct ``Affiliated_base_number`` this will give you a preview of the data that will be read in the upper right corner of the query editor.

```sql
SELECT COUNT(DISTINCT Affiliated_base_number) 
FROM `dtc-de-course-374821.fhv.external_fhv2`;
```

Step 3: Select the query to get the count of distinct ``Affiliated_base_number`` in the BQ table

```sql
SELECT COUNT(DISTINCT Affiliated_base_number)
FROM `dtc-de-course-374821.fhv.tripdata_csv`;
```

## Question 3:

Question 3:
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

- 717,748
- 1,215,687
- 5
- 20,332

Proposed solution:

```sql	
SELECT COUNT(*) as num_records
FROM `dtc-de-course-374821.fhv.tripdata_csv`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL
```

## Question 4:

What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

- Cluster on pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Partition by affiliated_base_number
- Partition by affiliated_base_number Cluster on pickup_datetime

Propose solution:

### Partitioning

BQ tables can be partitioned into multiple smaller tables. For example, if we often filter queries based on date, we could partition a table based on date so that we only query a specific sub-table based on the date we're interested in.

### Clustering

***Clustering*** consists of rearranging a table based on the values of its columns so that the table is ordered according to any criteria. Clustering can be done based on one or multiple columns up to 4; the ***order*** of the columns in which the clustering is specified is important in order to determine the column priority.

Clustering may improve performance and lower costs on big datasets for certain types of queries, such as queries that use filter clauses and queries that aggregate data.

Create the table with the partitioning and clustering:

```sql
CREATE OR REPLACE TABLE `dtc-de-course-374821.fhv.tripdata_patitioned_clusterd`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY  Affiliated_base_number AS
SELECT * FROM `dtc-de-course-374821.fhv.tripdata_csv`;
```

## Question 5:

Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

- 12.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
- 582.63 MB for non-partitioned table and 0 MB for the partitioned table
- 646.25 MB for non-partitioned table and 646.25 MB for the partitioned table

Proposed solution:

non-partitioned table:

```sql
SELECT DISTINCT Affiliated_base_number
FROM `dtc-de-course-374821.fhv.tripdata_csv`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';
```
652.6 MB for non-partitioned table

Partitioned table:

```sql
SELECT DISTINCT Affiliated_base_number
FROM `dtc-de-course-374821.fhv.tripdata_patitioned_clusterd`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';
```	
23.07 MB for partitioned table

## Question 6:
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Container Registry
- Big Table

Proposed solution:

In BigQuery, an external table is a table that is based on data that is stored outside of BigQuery, such as in Google Cloud Storage or a Google Drive. When you create an external table in BigQuery, the table definition and metadata are stored in the BigQuery catalog, while the data itself is stored in the external storage location.

***In our case is GCP Bucket***

## Question 7:
It is best practice in Big Query to always cluster your data:

While clustering your data in BigQuery can improve query performance and reduce costs, it is not always a best practice to cluster your data.

Clustering can be particularly useful when you frequently query your data by certain columns, such as dates or geographic regions. By clustering your data on these columns, you can reduce the amount of data that needs to be scanned during the query, which can result in faster query times and reduced costs.

However, clustering can also increase the cost of queries that do not use the clustered columns, as well as the cost of loading and streaming data into your table. Additionally, clustering can slow down table updates, as the clustering information must be updated each time new data is added to the table.