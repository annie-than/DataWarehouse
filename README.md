1. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud Amazon S3. So, we need to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.


2. State and justify your database schema design and ETL pipeline.

On Redshift: 
- Create tables:
	+ Create staging tables: staging_events, staging_songs
	+ Create a star schema optimized for queries on song play analysis.
		Fact Table: songplays 
		Dimension Tables: users, songs, artists, time 	
- Copy data from S3 to staging tables
	+ copy data from 's3://udacity-dend/log_data' to staging_events table
	+ copy data from 's3://udacity-dend/song_data' to staging_songs table
- Insert data to database schema
	+ From staging_events and staging_songs, insert data into Fact table and Dimension tables.


3. [Optional] Provide example queries and results for song play analysis.



