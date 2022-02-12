# -*- coding: utf-8 -*-
"""
Created on Sun Feb  6 12:19:23 2022

@author: dript
"""

import pandas as pd
import json
import urllib
from datetime import datetime,timedelta
from tqdm import tqdm


class District():
    
    def __init__(self,state_code):
        print("Fetching Data for Districts....")
        with open("district_code.json","r") as f:
            d_code = json.load(f)
    
        dis_data = {c.upper(): self.pull_district_data(c)
                for c in tqdm(state_code) if c.lower() != "tt"}
        
        self.prepared_dis_data = self.prepareDistrictDate(dis_data,d_code)
        self.prepared_dis_data_daily = self.dailyDataDistrict()
        
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
        
        
    def pull_district_data(self,state):
        url = f"https://data.covid19bharat.org/v4/min/timeseries-{state.upper()}.min.json"
    
        with urllib.request.urlopen(url) as url:
            district_data = json.loads(url.read().decode())
    
        target = district_data[state.upper()]
        return target.get("districts", None)
    
    def prepareDistrictDate(self,dis_data,d_code):
        return_data = {}
    
        for k in dis_data:
            dict_frame = {
                "confirmed": pd.DataFrame(),
                "deceased": pd.DataFrame(),
                "recovered": pd.DataFrame()
            }
            for st in dis_data[k]:
                if st.lower() != "unknown":
                    st_date = dis_data[k][st]["dates"]
                    st_date_list = list(st_date.keys())
                    for i in ["confirmed", "recovered", "deceased"]:
                        temp_dict = {}
                        temp_dict[st] = [
                            st_date[d]['total'].get(i, 0) if "total" in st_date[d].keys() else 0 for d in st_date]
                        t = pd.DataFrame(temp_dict, index=st_date_list)
                        dict_frame[i] = pd.concat([dict_frame[i], t], axis=1)
                        dict_frame[i] = dict_frame[i].fillna(0)
                        
            for df in dict_frame:
                df_temp = dict_frame[df]
                df_temp.index = pd.to_datetime(df_temp.index)
                df_temp.sort_index(inplace=True)
                df_temp.index = df_temp.index.strftime("%m/%d/%Y")
                df_temp_t = df_temp.T
                df_temp_t = df_temp_t.reset_index()
                df_temp_t = df_temp_t.rename(columns={"index": "DISTRICT"})
                df_temp_t.DISTRICT = df_temp_t.DISTRICT.str.lower()
                d_code_f = [d_code[k]["districts"].get(d.lower(),None) for d in df_temp_t.DISTRICT]
                df_temp_t.insert(loc = 1,column = "dt_code",value = d_code_f)
                dict_frame[df] = df_temp_t
    
            return_data[k] = dict_frame
        return return_data
    
    def dailyDataDistrict(self):
        prepared_dis_data_daily = {}
        
        for st,st_v in self.prepared_dis_data.items():
            if st not in prepared_dis_data_daily.keys():
                prepared_dis_data_daily[st] = {}
            for k,v in st_v.items():
                if not v.empty:
                    startIx = self.getdateStartIx(v)
                    date_frame = v.iloc[:,startIx:]
                    date_frame_diff = date_frame.diff(axis = 1)
                    date_frame_diff[date_frame_diff.columns[0]] = date_frame[date_frame.columns[0]]
                    date_frame_diff.insert(0,column = "DISTRICT",value = v["DISTRICT"])
                    date_frame_diff.insert(1,column = "dt_code",value = v["dt_code"])
                    prepared_dis_data_daily[st][k] = date_frame_diff
                else:
                    prepared_dis_data_daily[st][k] = v
        return prepared_dis_data_daily
    
    
    def getDistrictwiseData(self,date="12/30/2021"):
        def dis_wise_data(d_data):
            output = {
                "confirmed": pd.DataFrame(),
                "recovered": pd.DataFrame(),
                "deceased": pd.DataFrame(),
            }
        
        
            for st,st_data in d_data.items():
                for k in output.keys():
                    target = st_data[k]
                    if date not in list(target.columns):
                        target[date] = [0]*target.shape[0]
                    prevDate = datetime.strptime(date,"%m/%d/%Y") - timedelta(days = 1)
                    inc = target[date] - target[((datetime.strftime(prevDate,"%m/%d/%Y") in target.columns and datetime.strftime(prevDate,"%m/%d/%Y")) or date)]
                    
                    res_frame = target.loc[:,['DISTRICT','dt_code',date]]
                    res_frame["Inc"] = inc
                    
                    output[k] = pd.concat([output[k],res_frame])
            for k,v in output.items():
                v.columns = ['DISTRICT',"dt_code","COUNT","Inc"]
                output[k] = v.to_dict(orient="records")
            return output
        
        return {
                "cum": dis_wise_data(self.prepared_dis_data),
                "daily": dis_wise_data(self.prepared_dis_data_daily)
            }
    
    
    
    