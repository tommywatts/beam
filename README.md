# Data Engineer Tech Test

## Setup

Using Python 3.8.13 and the requirements set in requirements.txt.

Install requirements:

`pip install -r requirements.txt`

Install requirements to run unit tests (installs all):

`pip install -r requirements.test.txt`

## Overview

The pipelines directory contains two python files that hold beam pipelines for both tasks set. Both these can be run using the make commands provided below. They output to the output directory in the requested formats. 

Both pipelines take the same arguments and are run using the DirectRunner. The arguments are:

- --input - input file for the beam job, set to `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv` in the Makefile.
- --output - output location for when the job completes. set to `output/results` in the Makefile.

## Task1

Task 1 has been completed in `pipelines/beam_transactions.py`.

In this pipeline we have the following steps:

1. Read - Reads from the input given.
2. To Rows - ParDo that parses the csv into Rows using the CsvToRows class.
3. transaction_amount > 20 - filters out the elements in the pcollection where transaction_amount < 20.0.
4. timestamp > 2010 - filters out the elements in the pcollection where timestamp is earlier than 2010.
5. Group by date - groups by date and sums.

Run using the below command:

`make run-direct`

## Task2

Task 2 has been completed in `pipelines/beam_transactions_composite.py`.

In this pipeline we have the following steps:

1. Read - Reads from the input given.
2. To Rows - ParDo that parses the csv into Rows using the CsvToRows class.
3. Composite Transform - Combines the previous filters into one PTransform and aggregates on date, summing on transaction_amount. Uses the CompositeTransform class.

Run using the below command:

`make run-direct-composite`

Both the CsvToRows class and the CompositeTransform class are tested as required in `tests/test_pipeline.py`

## Output

Both beam pipelines output two files to the output directory:

1. results.csv - a csv that contains the output with headers, added as its more readable.
2. results.jsonl.gz - the zipped jsonl file that contains the results in lined json format.

## Tests

Tests are run using pytest with the command `make test-unit`.

There are two tests here, one for CsvToRows and one for CompositeTransform as mentioned previously.

I have also added dockerised tests to keep the environment contained which can be run using `make docker-test-unit`. 
