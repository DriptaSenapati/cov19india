# -*- coding: utf-8 -*-
"""
Created on Wed Feb  2 20:33:21 2022

@author: dript
"""

from api import ApiFetch
from districtapi import District
from datetime import datetime,timedelta
import warnings
warnings.filterwarnings("ignore")
from tqdm import tqdm
import os
import json
import urllib.request


def getLastUpdatedDate(url):
    with urllib.request.urlopen(url) as url:
        lastData = json.loads(url.read().decode())
        
    return lastData["UpdatedDate"]


def create(startDate,endDate,obj,dis_obj,curr_path):
    startDate = datetime.strptime(startDate,"%m/%d/%Y")
    endDate = datetime.strptime(endDate,"%m/%d/%Y")
    
    if "data" not in os.listdir(curr_path):
        os.mkdir(os.path.join(curr_path,"data"))
            
    sep_res_json = {
                "Charts": {
                        "cum": {k:v.to_dict(orient = "records") for k,v in obj.api_data.items()},
                        "daily": {k:v.to_dict(orient = "records") for k,v in obj.api_data_daily.items()}
                    },
                "States": obj.api_data['confirmed'][['STATE/UT','CODE']].to_dict(orient = "records")
                
            }
    
    with open(os.path.join(curr_path,"data","states.json"),"w") as f:
        json.dump(sep_res_json,f)
        
        
    while(startDate <= endDate):
        print(f"Fetching for {startDate.strftime('%m-%d-%Y')}.....")
        dashData = obj.getDashboardData(state = "TT",date = startDate.strftime('%m/%d/%Y'))
        statewiseData = obj.getStateWiseData(date = startDate.strftime('%m/%d/%Y'))
        tableData = obj.getTableDate(date = startDate.strftime('%m/%d/%Y'))
        
        dis_wise_data = dis_obj.getDistrictwiseData(date = startDate.strftime('%m/%d/%Y'))
        
        res_json = {
                "UpdatedTill":startDate.strftime('%m/%d/%Y'),
                "State": "TT",
                "dates": {"firstDate": obj.firstDate,"lastDate": obj.lastDate},
                "Dashboard": dashData,
                "Table": tableData,
                "Map": statewiseData,
                "District": dis_wise_data
            }
        
        
        
        
        curr_file_name = f"india_{startDate.strftime('%m-%d-%Y')}.json"
        with open(os.path.join(curr_path,"data",curr_file_name),"w") as f:
            json.dump(res_json,f)
            
        startDate = startDate + timedelta(days = 1)
        

def update(obj,dis_obj,curr_path):
    print("Checking any new date are found or not....")
    data_path = os.path.join(curr_path,"data")

    lastDate = getLastUpdatedDate(url = "https://driptasenapati.github.io/cov19india/data/india_current.json")
        
    save_date = datetime.strptime(lastDate,"%m/%d/%Y")
    api_date = datetime.strptime(obj.lastDate,"%m/%d/%Y")
    if api_date > save_date:
        print(f"New dates are found. Database is updating to {obj.lastDate}...")
        
        
        sep_res_json = {
            "Charts": {
                    "cum": {k:v.to_dict(orient = "records") for k,v in obj.api_data.items()},
                    "daily": {k:v.to_dict(orient = "records") for k,v in obj.api_data_daily.items()}
                },
            "States": obj.api_data['confirmed'][['STATE/UT','CODE']].to_dict(orient = "records")
            
        }

        with open(os.path.join(data_path,"states.json"),"w") as f:
            json.dump(sep_res_json,f)
        
        day_diff = (api_date - save_date).days
        for d in tqdm(range(0,day_diff + 1)):
            date = datetime.strftime(save_date + timedelta(days=d),"%m/%d/%Y")
            dashData = obj.getDashboardData(state = "TT",date = date)
            statewiseData = obj.getStateWiseData(date = date)
            tableData = obj.getTableDate(date = date)
            
            dis_wise_data = dis_obj.getDistrictwiseData(date = date)
            
            res_json = {
                    "UpdatedTill":date,
                    "State": "TT",
                    "dates": {"firstDate": obj.firstDate,"lastDate": obj.lastDate},
                    "Dashboard": dashData,
                    "Table": tableData,
                    "Map": statewiseData,
                    "District": dis_wise_data
                }
            curr_file_name = f"india_{(save_date + timedelta(days=d)).strftime('%m-%d-%Y')}.json"
            if(save_date + timedelta(days=d) == api_date):
                curr_file_name = "india_current.json"
                with open(os.path.join(curr_path,curr_file_name),"w") as f:
                    json.dump(res_json,f)
            
            with open(os.path.join(data_path,curr_file_name),"w") as f:
                json.dump(res_json,f)
    else:
        print("No update available")
    
    


if __name__ == "__main__":
    obj = ApiFetch()
    
    dis_obj = District(obj.api_data["confirmed"]["CODE"])
    update(obj,dis_obj,os.getcwd())
    #create(obj.firstDate,obj.lastDate,obj,dis_obj,os.getcwd())

    
    
