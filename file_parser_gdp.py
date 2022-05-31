#!/usr/bin/env python
# coding: utf-8
from datetime import datetime
import pandas as pd, json as js, argparse
import ingest_gdp_data

"""
dataset:

[
	{
		"page": 1,
		"pages": 1,
		"per_page": 5000,
		"total": 62,
		"sourceid": "2",
		"sourcename": "World Development Indicators",
		"lastupdated": "2022-04-27"
	},
	[
		{
			"indicator": {
				"id": "NY.GDP.MKTP.CD",
				"value": "GDP (current US$)"
			},
			"country": {
				"id": "IN",
				"value": "India"
			},
			"countryiso3code": "IND",
			"date": "2021",
			"value": null,
			"unit": "",
			"obs_status": "",
			"decimal": 0
		},
		{
			"indicator": {
				"id": "NY.GDP.MKTP.CD",
				"value": "GDP (current US$)"
			},
			"country": {
				"id": "IN",
				"value": "India"
			},
			"countryiso3code": "IND",
			"date": "2020",
			"value": 2660245248867.63,
			"unit": "",
			"obs_status": "",
			"decimal": 0
		}]
    ]"""

       
def generate_ctl(dataset):
	# reading metadata of json - first element in json array
	# sep will add column_name with underscore
	header = pd.json_normalize(dataset[0],sep = '_')
	
	#convert date to yymmdd for new file generation with date
	file_date = datetime.strptime(dataset[0]["lastupdated"],"%Y-%m-%d").strftime('%Y%m%d')
	header.to_csv(f'control_{file_date}.dat', index=False, sep=',')	
	return file_date
    
 
def normalize_data(dataset):
    # normalise the actual dataset - list of dictionaries
    rows_data = pd.json_normalize(dataset[1],sep = '_')
    return rows_data
    #print(rows_data)

def dq_apply(as_on_date,dataset):
    # apply dq - filter the record where value field has no data and store in a separate file
    rows_data = normalize_data(dataset)
    filtered_data = rows_data[rows_data['value'].isna()]
    filtered_data.to_csv(f'filtered_{as_on_date}.dat', index=False, sep=',')
    
	# take all good data - delete the not-required columns and store in final file to process
    clean_data = rows_data[rows_data['value'].notna()].drop(['obs_status','unit','decimal'],axis=1)
    clean_data.to_csv(f'gdp_{as_on_date}.dat', index=False, sep=',')

def ingest_data(gdp_data,filtered_data,control_data):
	ingest_gdp_data.load_data(["--user","gdp_db_user","--password","gdp1234","--host","localhost","--port",
	"5431","--db","gdp_db","--table_name","gdp_data","--filename","{fname}".format(fname=gdp_data),
	"--badfile","{bfile}".format(bfile=filtered_data),"--ctrlfile","{cfile}".format(cfile=control_data)])

def main(input_parameters):
	# Command line parser to receive file name as input
	parser = argparse.ArgumentParser(description='apply dq on gdp data')
	parser.add_argument('--inputFile', required=True, help='pass file to process..')
	args = parser.parse_args(input_parameters)
	
	file_input = args.inputFile
	
	with open(file_input) as input_file:
		dataset = list(js.load(input_file))
		as_on_date = generate_ctl(dataset)
		dq_apply(as_on_date,dataset)

		#ingest data to postgress
		ingest_data(f'gdp_{as_on_date}.dat',f'filtered_{as_on_date}.dat',f'control_{as_on_date}.dat')

# Start of the program
if __name__ == '__main__':
	main()   
