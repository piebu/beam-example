# BEAM TEST PROJECT

Simple test project using apache beam using Python3.

##INSTALLATION

Create a virtualenv:

`python3 -m venv /path/to/new/virtual/environment`

Install all the dependencies:

`pip3 install -r requirements.txt`

##INPUT FILE

The application accept csv files with the following format:

<pre>
timestamp,origin,destination,transaction_amount
2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99
2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95
</pre>


##USAGE

To execute run:  
`python3 main.py --input-file=path/to/inputfile --min_amount=20 --year=2021`

parameter accepted:  
* **input-file** (_optional_) default value: `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`  
* **min_amount** (_optional_) minimum transaction threshold default value `10`
* **year** (_optional_) Year to not consider default value `2010`







