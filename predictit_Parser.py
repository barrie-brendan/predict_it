#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 21 09:09:26 2019

@author: brendan
"""
import requests
import pandas as pd
import datetime
import mysql.connector
from sqlalchemy import create_engine

url = "https://www.predictit.org/api/marketdata/markets/3633"
response = requests.get(url)
jsonData = response.json()

data = jsonData.get("contracts")
df = pd.DataFrame(data)
df = df.drop(columns=['dateEnd', 'displayOrder', 'image', 'longName'])


timestamp = datetime.datetime.utcnow().strftime("%Y%m%d%H%M")
outputFileName = 'predictit_' + timestamp
df.to_csv(outputFileName, sep='\t', index=False)

engine = create_engine('mysql+mysqlconnector://user:pw@host:root/db')
df.to_sql(name=outputFileName, con=engine, if_exists='append', index=False)
