# Data Engineering Capstone Project

## Project Scope & The Data

I have chosen Amazon Kindle sales and review data, hosted on Kaggle based on the requirements of the project, namely - there must be 2 datasources (reviews and sales data), 2 formats (csv and json in this case), and the datasets must contain more than 1 million rows of data. 

Descriptive information on the 2 datasources can be found here:

https://www.kaggle.com/bharadwaj6/kindle-reviews
https://www.kaggle.com/ucffool/amazon-sales-rank-data-for-print-and-kindle-books

The two datasets can be modelled together based on the common identifier ASIN, which is Amazon's unique identifier.

### Purpose of the data model

I aim to model the data to support data analysis in areas such as:

- sentiment analysis of reviews
- analyse correlation of reviews and sales metrics
- highest rated and best selling kindle products.

My data analysis model will be a star schema, to create flexiblity and simplicity for carrying out data analysis. 

## Exploratory Data Analysis

### Kindle Sales Data

The datasource consists of 2 csv files, with around 60-70k rows containing information about the books such as the identifier, the group, format, title, author and publisher. Data quality initially looks good. Some observations:

unique identifier contains no nulls and is unique
group, format, title, author and publisher all contain some missing values
not all book titles are unique
there is a large number of different publishers, the most common publisher published 4% of books.

Sales Rank data is present in multiple JSON files.

# TODO: understand and desribe content and data quality of JSON files
66760 JSON files, with ASIN in their filename and timestamp and salesranks as key value pairs within the json.

asin's will need to be extracted from filename and created as a asin field

### Kindle Reviews Data
The datasource consists of 1 csv file with kindle review data such as  ASIN, helpfullness of review, overall review, reviewtext review time etc.

It is a very large csv, with 983K rows. Some observations of the data:

ASIN is unique and contains no nulls
Some review text has been flagged as mismatched by kaggle
There are no missing values.
Review times range beteen March 2000 and July 2014, with most data in the 2013-14 period.

- Document steps necessary to clean the data

## The Data Model

### TODO: Design and document the data model
- Map out the conceptual data model and explain why you chose that model

![ERD](media/erd.png)

I have chosen a snowflake schema, as it will support analysts to write analytical queries in less complex ways with less joins required. There is is some redundancy in the data as a consequence, which in this case is fine. In a web app backend I would 
normalize the data further.

### TODO: Plan ETL pipeline
- List the steps necessary to pipeline the data into the chosen data model
- Include this thinking for the project writeup:
Clearly state the rationale for the choice of tools and technologies for the project.
*RUBRIC The choice of tools, technologies, and data model are justified well*
Document the steps of the process.
Propose how often the data should be updated and why.

## The ETL pipeline

### TODO: Create the data pipelines and the data model
### TODO: Create a data dictionary
### TODO: Create and Run data quality checks to ensure the pipeline ran as expected
- Integrity constraints on the relational database (e.g., unique key, data type, etc.)
- Unit tests for the scripts to ensure they are doing the right thing
- Source/count checks to ensure completeness

## Project Writeup

- What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
- Clearly state the rationale for the choice of tools and technologies for the project.
    *RUBRIC The choice of tools, technologies, and data model are justified well*
- Document the steps of the process.
- Propose how often the data should be updated and why.

## Scenarios

- If the data was increased by 100x.
- If the pipelines were run on a daily basis by 7am.
- If the database needed to be accessed by 100+ people.