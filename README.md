## Udacity Data Engineer Nanodegree
## Data Warehouse
### Introduction and Project Purpose
A music streaming startup, Sparkify, has grown its user base and song database and want to move their processes and data onto the cloud. The company's data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in its app.

The company needs an ETL pipeline that extracts its data from S3, stages the data in Redshift, and transforms data into fact and dimensional tables for its analytics team to continue finding insights in what songs its users are listening to.

The project will require skills in data warehousing using AWS, and building ETL pipelines for a database hosted on Redshift. To complete the project, data will be loaded from S3 to staging tables on Redshift, and SQL statements will create analytical tables from the staging tables. A Redshift cluster will be built by applying the concept of **Infrastructure as Code (IaC)** using Amazon Web Services' Python Software Development Kit (SDK) `boto`.

### Datasets
There are two datasets that reside on S3 that will be loaded into staging tables on Redshift.

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`  

Log data json path: `s3://udacity-dend/log_json_path.json`

The **song dataset** is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

    song_data/A/B/C/TRABCEI128F424C983.json
    song_data/A/A/B/TRAABJL12903CDCF1A.json

Here is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

The **log dataset** consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset.

    log_data/2018/11/2018-11-12-events.json
    log_data/2018/11/2018-11-13-events.json

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

![log-data](img/log-data.png)

### Staging Tables
Before loading data into a Redshift schema, data on S3 will be loaded into staging tables.

**staging_events** - raw app activity log data files with song plays i.e. records with page `NextSong`

| column name   | data type | condition  |
| ------------- | --------- | ---------- |
| auth          | varchar   |            |
| firstName     | varchar   |            |
| gender        | char      |            |
| iteminSession | integer   |            |
| lastName      | varchar   |            |
| length        | numeric   |            |
| level         | varchar   |            |
| location      | varchar   |            |
| method        | varchar   |            |
| page          | varchar   |            |
| registration  | numeric   |            |
| sessionId     | integer   | sortkey    |
| song          | varchar   |            |
| status        | integer   |            |
| ts            | bigint    |            |
| userAgent     | varchar   |            |
| userId        | varchar   |            |

**staging_songs** - raw song data files containing metadata about a song and the artist of that song

| column name      | data type | condition |
| ---------------- | --------- | --------- |
| num_songs        | integer   |           |
| artist_id        | varchar   |           |
| artist_latitude  | numeric   |           |
| artist_longitude | numeric   |           |
| artist_location  | varchar   |           |
| artist_name      | varchar   |           |
| song_id          | varchar   | sortkey   |
| title            | varchar   |           |
| duration         | numeric   |           |
| year             | integer   |           |


### Data Warehouse Schema for Song Analysis
The data warehouse is designed using a **star schema** consisting of the following fact and dimension tables. The star schema is appropriate given the simplicity of the data model and the presence of one fact table with accompanying dimension tables. The star schema also supports the stated use case by the Sparkify analytics team to easily query data and quickly output aggregations.

#### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page `NextSong`

| column name | data type | condition                 |
| ----------- | --------- | ------------------------- |
| songplay_id | integer   | identity(0,1) primary key |
| start_time  | timestamp | not null sortkey          |
| user_id     | integer   | not null                  |
| level       | varchar   |                           |
| song_id     | varchar   |                           |
| artist_id   | varchar   |                           |
| session_id  | integer   | not null                  |
| location    | varchar   |                           |
| user_agent  | varchar   |                           |


#### Dimension Tables  
**users** - users in the app

| column name | data type | condition           |
| ----------- | --------- | ------------------- |
| user_id     | integer   | primary key sortkey |
| first_name  | varchar   |                     |
| last_name   | varchar   |                     |
| gender      | char      |                     |
| level       | varchar   | distkey             |


**songs** - songs in music database  

| column name | data type | condition           |
| ----------- | --------- | ------------------- |
| song_id     | varchar   | primary key sortkey |
| title       | varchar   | not null            |
| artist_id   | varchar   | not null            |
| year        | integer   | distkey             |
| duration    | numeric   |                     |


**artists** - artists in music database   

| column name | data type | condition |
| ----------- | --------- | --------- |
| artist_id   | varchar   | sortkey   |
| name        | varchar   | not null  |
| location    | varchar   | distkey   |
| latitude    | float     |           |
| longitude   | float     |           |


**time** - timestamps of records in `songplays` broken down into specific units

| column name | data type | condition                   |
| ----------- | --------- | --------------------------- |
| start_time  | timestamp | primary key sortkey distkey |
| hour        | integer   |                             |
| day         | integer   |                             |
| week        | integer   |                             |
| month       | integer   |                             |
| year        | integer   |                             |
| weekday     | integer   |                             |

### Project Structure
The project consists of the following Python scripts:

1. `launch_redshift.py`
 - When called, this script will instantiate a new IAM role and Redshift cluster based on the parameter configurations specified in the `dwh.cfg` file. Upon building the data warehouse cluster, the script will print to the console the cluster endpoint and IAM role Amazon Resource Name (ARN). These two variables are then added to the `dwh.cfg` file, and the AWS access and secret keys are then removed.  


2. `sql_queries.py`
 - Includes `DROP`, `CREATE`, `COPY`, and `INSERT` statements for the necessary staging, fact, and dimension database tables. Query statements from this script are called in the `create_tables.py` and `etl.py` scripts.


3. `create_tables.py`
 - Once the Redshift cluster is instantiated and the cluster endpoint and IAM role ARN are identified, run this script to connect to the database and create the requisite staging, fact, and dimension tables specified in `sql_queries.py`. This script can be re-run to reset the database and test the ETL pipeline.


4. `etl.py`
 - This script will ingest song and log data from S3 into the song and log staging tables, and then insert processed and cleaned data into the database fact and dimension tables. This script should be run after executing `create_tables.py`.


5. `take_down_redshift.py`
 - This script is optional and should be run last to delete the created Redshift and IAM role resources identified in `dwh.cfg`.  


### Conclusion
Transitioning Sparkify's data storage and analytical engine to the Cloud positions the company for long-term data management success. The power and scalability of AWS will allow the company to easily increase or reduce infrastructure needs as the business grows and analytical needs are refined. AWS provides several options for applying Infrastructure as Code strategies, and is continually adding new data services to its suite of tools and functionality. The successful completion of this project marks the starting point of advanced data engineering at Sparkify, and opens to door to endless possibilities.


### Example Queries
The number of song plays by hour to understand when during the day users are using the app more frequently - app usage appears to peak between 3-6pm.

    select
    t.hour,
    count(distinct sp.songplay_id) as song_plays
    from songplays sp
    join time t on t.start_time = sp.start_time
    group by t.hour
    order by t.hour;

The number of song plays by subscription level - paid subscribers are more active.

    select
    level,
    count(distinct songplay_id) as song_plays
    from songplays
    group by level;

The count of users by gender - there are more female users than male.

    select
    gender,
    count(distinct user_id)
    from users
    group by gender;