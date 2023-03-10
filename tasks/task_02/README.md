# Task 2

## Instructions
Implementation: Input data is a jsonl file, for the assignment purpose the desired ouput should be a csv file. Feel free to use any programming language you prefer.

Evaluation: Your work will be evaluated based on the completeness of the ETL process, the accuracy of the data transformation, the efficiency of the ETL code, and the quality of the documentation and test results.

You can use the `Solution` section to add notes about your implementation, add files containing the solution inside the current folder along instructions about the ETL execution.

### Question

Given the dataset inside the folder `data` implement an ETL that clean and flatten data removing duplicates and add the following columns:

year (YYYY)
year-month (YYYYMM) zero filled
year-quarter (YYYYQ#)
year-week (YYYYWXX) ISO week zero filled
weekday (1:mon-7:sun)

# Solution
# ETL Pipeline for Orders Data

This repository contains an ETL (Extract, Transform, Load) pipeline   
that processes raw orders data in JSON format and outputs a filtered 
dataset in CSV format.

The ETL pipeline reads the orders data from a JSON file, 
applies various data transformations to it, and outputs the filtered data 
as a CSV file.

## Prerequisites

To run the ETL pipeline, you'll need the following software:

- Python 3.7 or higher
- Apache Spark 3.2.0 or higher

You can install the required Python packages by running the following command:

<code>pip install -r requirements.txt</code>

## Project structure
The repository contains the following files:

- __etl_pipeline.py__: The main Python module containing the ETL pipeline code.
- __metadata.yaml__: A YAML file containing metadata information about the dataset.
- __test_etl_pipeline.py__: Contains unit tests for the ETL pipeline.

## Usage
To run the ETL pipeline, execute the `save_dataframe_to_csv` function in the `etl_pipeline.py`   
module using the `spark-submit` command:  

- <code>spark-submit etl_pipeline.py</code>  

Alternatively, you can run the pipeline using Python by executing 
the `etl_pipeline.py` module directly:

- <code>python etl_pipeline.py</code>

Note that running the pipeline with `spark-submit` is recommended 
for larger datasets, as it can take advantage of Spark's distributed computing capabilities to process data more efficiently.