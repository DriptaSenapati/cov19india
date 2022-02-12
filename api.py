# -*- coding: utf-8 -*-
"""
Created on Wed Feb  2 20:10:25 2022

@author: dript
"""

import pandas as pd
import json
from datetime import datetime,timedelta

class ApiFetch():
    
    API_CONFIG = {
        "confirmed": "https://raw.githubusercontent.com/kalyaniuniversity/COVID-19-Datasets/master/India%20Statewise%20Confirmed%20Cases/COVID19_INDIA_STATEWISE_TIME_SERIES_CONFIRMED.csv",
        "recovered": "https://raw.githubusercontent.com/kalyaniuniversity/COVID-19-Datasets/master/India%20Statewise%20Recovery%20Cases/COVID19_INDIA_STATEWISE_TIME_SERIES_RECOVERY.csv",
        "deceased": "https://raw.githubusercontent.com/kalyaniuniversity/COVID-19-Datasets/master/India%20Statewise%20Death%20Cases/COVID19_INDIA_STATEWISE_TIME_SERIES_DEATH.csv"
    }
    
    population = None
    lastDate = None
    firstDate = "02/01/2020"
    
    def __init__(self) -> None:
        print("Fetching Data from GitHub....")
        self.api_data = {}
        for key in self.API_CONFIG:
            read_data = pd.read_csv(self.API_CONFIG[key])
            read_data["CODE"] = read_data["CODE"].str.upper()
            self.api_data[key] = self.correct_date_formats(read_data)
        self.processFetchedData()
        self.api_data_daily = self.getDailyData()
        
        
    def getdateStartIx(self,api_data_frame):
        cols = api_data_frame.columns
        ix = None
        for j in cols:
             try:
                 date = datetime.strptime(j, "%m/%d/%Y")
                 ix = list(cols).index(j)
                 break
             except:
                 continue
        return ix
    
    def processFetchedData(self):
        for k,v in self.api_data.items():
            if self.population == None:
                popdata = v[["STATE/UT","CODE","POPULATION"]]
                popdata = popdata[popdata.CODE != 'TT']
                popdata.POPULATION = popdata.POPULATION.str.replace(",","").astype(int)
                self.population = popdata.to_dict(orient = "records")
            left = v[["STATE/UT","CODE"]]
            right = v.loc[:,self.firstDate:]
            final_frame = pd.concat([left,right],axis = 1)
            if self.lastDate == None:
                self.lastDate = final_frame.columns[-1]
            self.api_data[k] = final_frame
            


    def correct_date_formats(self,api_data):
        getIx = self.getdateStartIx(api_data)
        prevCols = []
        if getIx > 0:
            prevCols = list(api_data.columns)[:getIx]
        cols = list(api_data.columns)[getIx:]
        cols = [datetime.strptime(j,"%m/%d/%Y") for j in cols]
        cols = [datetime.strftime(j,"%m/%d/%Y") for j in cols]
        api_data.columns = prevCols + cols
            
            
        return api_data
    
    def getDailyData(self,data = None):
        api_data_daily = {}
        
        
        if data == None:
            data = self.api_data
        
        for key in data:
            k_data = data[key]
            dateIx = self.getdateStartIx(k_data)
            date_frame = k_data.iloc[:,dateIx:]
            date_frame_diff = date_frame.diff(axis = 1)
            date_frame_diff[date_frame_diff.columns[0]] = date_frame[date_frame.columns[0]]
            date_frame_diff.insert(0,column = "STATE/UT",value = k_data["STATE/UT"])
            date_frame_diff.insert(1,column = "CODE",value = k_data["CODE"])
            api_data_daily[key] = date_frame_diff
            
        return api_data_daily
    
    def getDashboardData(self,state,date = None,days = 30):
        if not date:
            date = self.lastDate
        
        code = state.upper()
            
        def generate_dash_data(data):
            dash_data = {}
            dash_data_last_days = {}
            for key in data:
                k_data = data[key]
                dash_data[key] = {
                    "COUNT":k_data[k_data["CODE"] == code][date].tolist()[0],
                    "Increment": k_data[k_data["CODE"] == code][date].tolist()[0] - k_data[k_data["CODE"] == code][k_data.columns[-2]].tolist()[0]
                }
                diffdays = datetime.strptime(date,"%m/%d/%Y") - datetime.strptime(self.firstDate,"%m/%d/%Y")
                if diffdays.days >= days:
                    startD = (datetime.strptime(date,"%m/%d/%Y") - timedelta(days = days)).strftime("%m/%d/%Y")
                    dash_data_last_days[key] = k_data[k_data["CODE"] == code].loc[:,startD:date].iloc[0,:].tolist()
            
            return {
                "LatestDate":date,
                "Response": dash_data,
                "Lastdays": dash_data_last_days
            }
        return {
                "cum": generate_dash_data(self.api_data),
                "daily": generate_dash_data(self.api_data_daily)
            }
    
    def getStateWiseData(self,date = None):
        
        
        if date == None:
            date = self.lastDate
        
        def stateDate(data):
            stateData = {}
            for k,v in data.items():
                v.CODE = v["CODE"].str.upper()
                
                res_data = v[['STATE/UT','CODE',date]]
                prevDate = datetime.strptime(date,"%m/%d/%Y") - timedelta(days = 1)
                inc = v[date] - v[((datetime.strftime(prevDate,"%m/%d/%Y") in v.columns and datetime.strftime(prevDate,"%m/%d/%Y")) or date)]
                res_data.columns = ['STATE/UT',"CODE","COUNT"]
                res_data["Inc"] = inc
                res_data = res_data[res_data['CODE'] != 'TT']
                res_data.sort_values(by='COUNT', ascending=False)
                stateData[k] = json.loads(res_data.to_json(orient = "records"))
                
            return stateData
        
        return {
                "cum": stateDate(self.api_data),
                "daily": stateDate(self.api_data_daily)
            }
    
    def getTableDate(self,date = None):
        if date == None:
            date = self.lastDate
            
        def table_data(data):
            dataP = pd.DataFrame()
            cols = []
            for k,v in data.items():
                cols.append(k.title())
                if dataP.empty:
                    dataP = pd.concat([dataP,v[['STATE/UT','CODE',date]]],axis = 1).set_index(['STATE/UT','CODE'])
                else:
                    dataP = dataP.join(v[['STATE/UT','CODE',date]].set_index(['STATE/UT','CODE']))
                dataP.columns = cols
            return dataP.reset_index()
        
        return {
                "cum": table_data(self.api_data).to_dict(orient = "records"),
                "daily": table_data(self.api_data_daily).to_dict(orient = "records")
            }
        
        
    
    