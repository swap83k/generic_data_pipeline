#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import doctest
import psutil
import ijson
import json

#json_name = 'PatientData.json'

def get_analysis_from_BMI(bmi):    
    """Return the analysis from bmi.

    >>> get_analysis_from_BMI(20)
    ('Normal weight', 'Low risk')
    >>> get_analysis_from_BMI(30)
    ('Moderately obese', 'Medium risk')
    """

    if bmi <=18.4:
        return('Underweight','Malnutrition risk')
    elif bmi >=18.5 and bmi <= 24.9:
        return('Normal weight','Low risk')
    elif bmi >=25 and bmi <= 29.9:
        return('Overweight','Enhanced risk')
    elif bmi >=30 and bmi <= 34.9:
        return('Moderately obese','Medium risk')
    elif bmi >=35 and bmi <= 39.9:
        return('Severely obese','High risk')
    elif bmi >=40:
        return('Very severely obese','Very high risk')
        
def analyse_using_df():   
    
    df_iter = pd.read_json(json_name)
    
    df_iter["BMI"] = (df_iter.WeightKg.div(df_iter.HeightCm.div(100).pow(2)))
    
    df_iter["BMI_Category"]  = df_iter.apply(lambda x: get_analysis_from_BMI(x.BMI)[0],axis=1)

    df_iter["Health risk"]  = df_iter.apply(lambda x: get_analysis_from_BMI(x.BMI)[1],axis=1)

    #print(df_iter)

    output_file = open('G:\Git repos\generic_data_pipeline\PatientResultsDf.json', 'w', encoding='utf-8')

    df_iter.to_json(output_file,orient='records')
        
    #print('Validated number of people with Overweight: ' + str(df_iter.query('BMI_Category == "Overweight"').shape[0]))

    doctest.testmod()

    print('RAM usage is {} MB'.format(int(int(psutil.virtual_memory().total - psutil.virtual_memory().available)/ 1024 / 1024)))

    print('RAM usage is {} %'.format(psutil.virtual_memory().percent))

def ijson_processor():  

    #for prefix, event, value in ijson.parse(open(json_name)):
        #print('prefix={}, event={}, value={}'.format(prefix, event, value))

    output_file = open('G:\Git repos\generic_data_pipeline\PatientResultsIjson.json', 'w', encoding='utf-8')

    with open('G:\Git repos\generic_data_pipeline\PatientData.json','r') as f:
       objects = ijson.items(f, 'item')
       #print(objects)   
       columns = list(objects)
       #print(columns)
       for idx in range(len(columns)):
           #print(idx)
           columns[idx]['BMI'] = (columns[idx]['WeightKg']/(columns[idx]['HeightCm']/100)**2)
           columns[idx]["BMI_Category"]  = get_analysis_from_BMI(columns[idx]['BMI'])[0]
           columns[idx]["Health risk"] = get_analysis_from_BMI(columns[idx]['BMI'])[1] 
                      
            
    #print(columns)

    json.dump(columns, output_file)    
    
    print('RAM usage1 is {} MB'.format(int(int(psutil.virtual_memory().total - psutil.virtual_memory().available)/ 1024 / 1024)))

    print('RAM usage1 is {} %'.format(psutil.virtual_memory().percent))

if __name__ == '__main__':
    json_name = 'G:\Git repos\generic_data_pipeline\PatientData.json'
    analyse_using_df()
    ijson_processor()