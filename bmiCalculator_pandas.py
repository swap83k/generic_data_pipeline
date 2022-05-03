#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import doctest
import psutil


json_name = 'PatientData.json'

df_iter = pd.read_json(json_name)

df_iter["BMI"] = (df_iter.WeightKg.div(df_iter.HeightCm.div(100).pow(2)))

df_iter["BMI_Category"]  = df_iter.apply(lambda x: get_analysis_from_BMI(x.BMI)[0],axis=1)

df_iter["Health risk"]  = df_iter.apply(lambda x: get_analysis_from_BMI(x.BMI)[1],axis=1)

print(df_iter)

print('Validated number of people with Overweight: ' + str(df_iter.query('BMI_Category == "Overweight"').shape[0]))

doctest.testmod()

print('RAM usage is {} MB'.format(int(int(psutil.virtual_memory().total - psutil.virtual_memory().available)/ 1024 / 1024)))

print('RAM usage is {} %'.format(psutil.virtual_memory().percent))

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


