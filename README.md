# Purpose: 

To build a data model to support analysis of stock market and real estate data. Specifically, to see what correlations, if any, can be drawn between specific stock performances and real estate trends. I forsee this data being useful for contructing analytical models, reports, and tables for drawing conclusions regarding this topic.

# Scope: 

### Stock/ETF
For this model, we will use daily stock and ETF data from 1970-2017, containing the opening price, highest price, lowest price, closing price and volume. 

### Homes
We will also use monthly home values, home values per sqft, home sales prices, home sales count, home sales list prices, and home sales list price per sqft from varying data ranges between 1996-2019. All values are medians. 

### Companies
Finally, I've tied in Company data for the Stock/ETF data so that it is easier to draw correlations between secondary business data like sectors and industries and real estate trends.

# Tools:

Airflow easily integrates with Redshift thanks to the Redshift Operators. Together, Airflow will be able to easily coordinate the Redshift operations to load and transform the data to it's final schema. 

**Coordination:** Airflow

**Data Prep:** Python

**ELT/Storage:** Redshift

# How It Works:

The project follows the following steps:
1. Prepare the local Data by doing a light clean up/prep of the content and format of the files. The transformed copy is saved to an Ouput directory for further processing. (If the transformed files already exist on your local system in the Output directory then this step is skipped)
2. Validate that the transformed files all exist and aren't empty.
3. Copy the transformed files to S3. (The S3 bucket is provided, you just need to provide credentials so you can access it. Also, if the files already exist in S3, this step is skipped))
4. Validate that the files all exist in S3.
5. Copy the S3 transformed files to the staging tables in Redshift.
6. Validate that the staging tables are all populated.
7. Transfer the staging data to the dimension tables, doing transformations to match the dimension table layouts.
8. Validate that the dimension tables are all populated.
9. Tranfer the staging data to the fact tables, doing transformations along the way to match the fact table layouts (The dimension tables are also used to assist in this, which is why we validate that the dimension tables are created first) 
10. Validate that the fact tables are all populated.

# How to Run:

To run this project, you will need to complete the below steps:

1. Have a Airflow instance, AWS Redshift cluster, and AWS key with secret ready to be utilized.
2. Ensure that you have GIT LFS (Large File Storage) installed before you pull down the repo so you can get the Data.zip file. 
3. Clone the repo (I'd recommend this over downloading the ZIP due to the use of GIT LFS)
4. Copy the dag and plugin direcotries to you airflow directory
5. Uncompress the Data files to your HOME directory in Linux (should be $HOME/Data/Companies, $HOME/Data/Homes, $HOME/Data/Stocks, $HOME/Data/ETFs when uncompressed)
6. In the Airflow WebUI, setup the following connections: 

	- A Postgres connection named "redshift" that matches your redshift db credentials.
	- A S3 connection named "data_dest" with your AWS key and secret as the login and password.
	- An Amazon Wed Services (AWS) connection named "airflow\_credentials" with your AWS key and secret as the login and password.
	
7. Turn on the stock\_home\_dag DAG.
8. Track it's progress and use the data quality task logs to verify that the tables and temp directories ar being populated properly.

# Data Outline:

### Data Directories

The Data directory is broken into 4 parts: ETFs, Stocks, Companies, and Homes. Each directory holds their respective files types. 

### Data Format
The ETF and Stock file names start with the company abbreviation end with .us. The file contents are CSV format and contain the Dates, Open Prices, Highs, Lows, Closing Prices, Volume and an integer indicating if the stock/etf is open or not for both. The home data is split across 6 CSV files: Sale Counts, Sale Prices, Listing Price, Listing Price per Sqft, Home Value, and Home Value per Sqft. Each row contains Zillow's RegionIds, the Region name (state's in this case), SizeRanking (which ranks based on the file type), and then the date range that the file covers makes up the rest of the columns. There is only 1 company file in the Companies directory. It is a CSV file that contains all off the Company data related to the stocks and ETFs.

### Data Preperation

Due to all of the data's usnique data formats, the data will need to go through a single prepatory transformation so it can be inserted into the staging tables in Redshift. The Company Abbreviations will need to be added to each row of every Stock and ETF file. The Home files will need to be mapped to JSON formats to allow for captruing the date range columns as values to be used in our queries. I also ensure that all valaues for the home data are a float. So, I replace any NaN Null values with 0.0 and cast all other values to floats. 

### Data Validation

After the ETL/ELT pipe lines are run, general checks are done regarding validation counts for each table. As well, spot checking will be done to ensure that all values were transformed properly and there aren't nulls.

### Data Updates

There are currently no plans to update the data. However, if updates were made, no changes would have to be made to any part of the process and you could just add the new files to the established directories and run the pipeline.

### Total Data Counts:

When run, you should have 104,829,500 total records accross all tables with the below breakdown from roughly 791 MB worth of initial data files.

- 87,307,689 records in the fact tables
- 20,737 records in the diminesion tables
- 17,501,085 records in the staging tables

These numbers should be verifiable via the logs of the data quality check tasks for each section as well as basic count queries that you can run on your cluster after the load is completed.

# Data Model:

While there are many more fact tables compared to dimensions tables, I feel this star model is best suited for this data to allow for fast comparison of various stock and real estate metrics based on dates (the single dimension that stretches across all facts). The staging tables for the home data will be created and deleted every run. Each dimension table will discard any duplicate rows on insertion. Each fact table will upsert, overwriting duplicates.

### Schema: 
stock\_home\_data

### Tables:

**staging\_sales\_count** *(Staging)*:

state varchar, date varchar, value float

**staging\_sales\_price** *(Staging)*:

state varchar, date varchar, value float

**staging\_listing\_price** *(Staging)*:

state varchar, date varchar, value float

**staging\_listing\_price\_sqft** *(Staging)*:

state varchar, date varchar, value float

**staging\_home\_values** *(Staging)*:

state varchar, date varchar, value float

**staging\_home\_values\_sqft** *(Staging)*:

state varchar, date varchar, value float

**staging\_stocks** *(Staging)*:

Company\_Abbr varchar, Date varchar, Open float, High float, Low float, Close float, Volume bitint, OpenInt int

**staging\_etf** *(Staging)*:

Company\_Abbr varchar, Date varchar, Open float, High float, Low float, Close float, Volume bitint, OpenInt int

**staging\_companies** *(Staging)*:

ticker varchar, company\_name varchar, short\_name varchar, industry varchar, description varchar, website varchar, logo varchar, ceo varchar, exchange varchar, market\_cap varchar, sector varchar, tag\_1 varchar, tag\_2 varchar, tag\_3 varchar

**Home\_Value** *(Fact)*: primary key(state\_id,date\_id)

state\_id int, date\_id int, home\_value float

**Home\_Value\_Per\_Sqft** *(Fact)*: primary key(state\_id,date\_id)

state\_id int, date\_id int, value\_per\_sqft float

**Home\_Listing\_Price** *(Fact)*: primary key(state\_id,date\_id)

state\_id int, date\_id int, listing\_price float

**Home\_Listing\_Price\_Per\_Sqft** *(Fact)*: primary key(state\_id,date\_id)

state\_id int, date\_id int, listing\_price\_per\_sqft float

**Home\_Sales\_Price** *(Fact)*: primary key(state\_id,date\_id)

state\_id int, date\_id int, sales\_price float

**Home\_Sales\_Count** *(Fact)*: primary key(state\_id,date\_id)

state\_id int, date\_id int, sales\_count float

**Stock\_Open\_Prices** *(Fact)*: primary key(company\_abbr, date\_id)

company\_abbrv varchar, date\_id int, open\_price float

**Stock\_Highs** *(Fact)*: primary key(company\_abbr, date\_id)

company\_abbrv varchar, date\_id int, high\_value float

**Stock\_Lows** *(Fact)*: primary key(company\_abbr, date\_id)

company\_abbrv varchar, date\_id int, low\_value float

**Stock\_Closing\_Prices** *(Fact)*: primary key(company\_abbr, date\_id)

company\_abbrv varchar, date\_id int, close\_price float

**Stock\_Volumes** *(Fact)*: primary key(company\_abbr, date\_id)

company\_abbrv varchar, date\_id int, volume float

**ETF\_Open\_Prices** *(Fact)*: primary key(company\_abbr, date\_id)

company\_abbrv varchar, date\_id int, open\_price float

**ETF\_Highs** *(Fact)*: primary key(company\_abbr, date\_id)

company\_abbrv varchar, date\_id int, high\_value float

**ETF\_Lows** *(Fact)*: primary key(company\_abbr, date\_id)

company\_abbrv varchar, date\_id int, low\_value float

**ETF\_Closing\_Prices** *(Fact)*: primary key(company\_abbr, date\_id)

company\_abbrv varchar, date\_id int, close\_price float

**ETF\_Volumes** *(Fact)*: primary key(company\_abbr, date\_id)

company\_abbrv varchar, date\_id int, volume float

**Company** *(Dimension)*: primary key(company\_abbr)

company\_abbrv varchar, full\_name varchar, short\_name varchar, industry varchar, sector varchar, tag\_1 varchar, tag\_2 varchar, tag\_3 varchar

**State** *(Dimension)*: primary key(state\_id)

state\_id int, state\_name varchar

**Date** *(Dimension)*: primary key(date\_id)

date\_id int, date\_chars varchar, date\_year int, date\_month int, date\_day int

# Alternative Scenarios:

1. If the data were increased by 100x, I would make the shift to using Spark instead of Redshift for my storage needs. The amount of data processed is already fairly high (> 107 million records), so 100x the data would require a transition to a big data solution such as Spark to handle storage and processing.
2. If the pipeline would need to be run on a daily basis by 7am everyday, I would simply schedule my Airflow job to begin processing at 12am as well as add a clean up activity to ensure that any data moved to staging was removed from the file system to ensure a clean directory for the next batch of files.
3. If the database needed to be accessed by 100+ people, I would again shift to Spark over Redshift due to it's low latency when processing multiple parallel queries.
