
print('------------- LM DASHBOARD: Greater Jakarta -----------------------')

import os
import requests
import time
from pprint import pprint
import json
import pandas as pd
import sys
from datetime import datetime,timedelta
start = datetime.now()
import numpy as np
import gspread as gs
import df2gspread as d2g
import gspread_dataframe as gd
import urllib

credentials ={
}
gc = gs.service_account_from_dict(credentials)

# Pull Redash Func
def poll_job(s, redash_url, job):
    # TODO: add timeout
    while job['status'] not in (3,4):
        response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
        job = response.json()['job']
        time.sleep(1)

    if job['status'] == 3:
        return job['query_result_id']
    
    return None


def get_fresh_query_result(redash_url, query_id, api_key, params):
    s = requests.Session()
    s.headers.update({'Authorization': 'Key {}'.format(api_key)})

    payload = dict(max_age=0, parameters=params)

    response = s.post('{}/api/queries/{}/results'.format(redash_url, query_id), data=json.dumps(payload))

    if response.status_code != 200:
        return 'Refresh failed'
        raise Exception('Refresh failed.')

    result_id = poll_job(s, redash_url, response.json()['job'])

    if result_id:
        response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url, query_id, result_id))
        if response.status_code != 200:
            raise Exception('Failed getting results.')
    else:
        raise Exception('Query execution failed.')

    return response.json()['query_result']['data']['rows']

# Export to Gsheet Func
def export_to_sheets(file_name,sheet_name,df,mode='r'):
    ws = gc.open(file_name).worksheet(sheet_name)
    if(mode=='w'):
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=df,include_index=False,include_column_header=True,resize=True)
        return True
    elif(mode=='a'):
        #ws.add_rows(4)
        old = gd.get_as_dataframe(worksheet=ws)
        updated = pd.concat([old,df])
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=updated,include_index=False,include_column_header=True,resize=True)
        return True
    else:
        return gd.get_as_dataframe(worksheet=ws)


print('>> Pulling Data from Redash 2084....')
loopcounter = 0
while True:
    try:
        params = {'region':["Greater Jakarta"]}
        api_key = 'KGy4OJdamBVI2gUWdjBfKgj8cpUETJwVqZPwb2Yf'
        result = get_fresh_query_result('https://redash-id.ninjavan.co/',2084, api_key, params)
        print('>> DONE')
        break
    except:
        print('Pulling data failed. Retrying..')

active_orders = pd.DataFrame(result)


datetime_col = ['creation_datetime','arrived_at_dest_datetime','refreshed_at']

active_orders[datetime_col] = active_orders[datetime_col].astype('datetime64[s]')
print('Total Active Orders: ',len(active_orders))

############## Attempted Today Orders ##############################################
print('>> Pulling Attempted Today Order from Redash...')
loopcounter = 0
while True:
    try:
        params = {'region_name':'"Greater Jakarta"', 'rts':["0"]}
        api_key = 'KGy4OJdamBVI2gUWdjBfKgj8cpUETJwVqZPwb2Yf'
        result = get_fresh_query_result('https://redash-id.ninjavan.co/',2091, api_key, params)
        print('>> DONE')
        break
    except:
        print('Pulling data failed. Retrying..')
        # loopcounter = loopcounter +1
        # if loopcounter >=5:
        #     break
            
attempted_today = pd.DataFrame(result)
print('Total Order Attempted Today: ',len(attempted_today))

attempted_today = attempted_today.reindex(columns = active_orders.columns)

# append
# raw_data = active_orders.append(attempted_today, ignore_index=True)
raw_data = pd.concat([active_orders,attempted_today], ignore_index=True)

############## Get PRIOR tag and Last Attempt datetime
# Get PRIOR order tag
list_orderid = raw_data['order_id'].unique().tolist()
listOrderID = ",".join(map(str, list_orderid))

print('>> Pulling PRIOR tag data from Redash 493...')
loopcounter = 0
while True:
    try:
        params = {'order_id':listOrderID, 'tag_name': ["PRIOR"]}
        api_key = 'your key'
        result = get_fresh_query_result('https://redash-id.ninjavan.co/',493, api_key, params)
        print('>> DONE')
        break
    except:
        print('Pulling data failed. Retrying..')


order_prior = pd.DataFrame(result)

# Join data
raw_data = pd.merge(raw_data, order_prior, on='order_id', how='left')


# get last_attempt
list_orderid = raw_data['order_id'].unique().tolist()
listOrderID = ",".join(map(str, list_orderid))

raw_data = raw_data[raw_data['dest_area'] != '']

print('>> Pulling last attempt data from Redash...')
loopcounter = 0
while True:
    try:
        params = {'order_id':listOrderID}
        api_key = 'your key'
        result = get_fresh_query_result('https://redash-id.ninjavan.co/',2144, api_key, params)
        print('>> DONE')
        break
    except:
        print('Pulling data failed. Retrying..')

            
df_lastatt = pd.DataFrame(result)
raw_data = pd.merge(raw_data, df_lastatt[['order_id','last_attempt']], on='order_id', how='left')

raw_data.rename(columns={'granular_status_redash':'granular_status'},inplace=True)

# Aging since LM Inbound
raw_data['aging'] = (raw_data['refreshed_at']-raw_data['arrived_at_dest_datetime']).dt.days
raw_data['aging_days'] = np.where(raw_data['aging'] > 5, '>5', raw_data['aging'])

# Setup Raw Data
raw_data = raw_data[['order_id','tracking_id','granular_status','parcel_size','creation_datetime','arrived_at_dest_datetime'
                    ,'dest_hub','dest_area','dest_region','dest_zone','total_attempts','aging_days','last_attempt'
                      ,'tag_name','tag_creation_date','refreshed_at']]



raw_data = raw_data.drop_duplicates(subset='tracking_id',keep='first')


########### Upload to Google Sheets######################################

print('>> Upload to Google Sheets...')
countinject = 0
while True:
    try:
        inject = export_to_sheets("Raw Data LM Dashboard - Greater Jakarta", 'Active Orders', raw_data, mode='w')
        print('>> Upload to Google Sheet DONE')
        break
    except:
        print('>> Failed. Retrying...')

            
end = datetime.now()
code_runtime = (end - start).total_seconds()/60
print("Elapsed Time: ", code_runtime, 'minutes')

########################## Active PETS Ticket#############################
print(' ')
print('>>>>>>>>>>> PETS TICKET')
# end_date is today's date
today = datetime.now()
end_date = today
start_date = end_date - timedelta(days=90)

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

dts = [dt.strftime('%Y-%m-%d') for dt in 
       datetime_range(start_date, end_date, timedelta(days=30))]
dts.append(end_date.strftime('%Y-%m-%d'))

dataframes = []
print(dts)

print('>> Pulling Data from Redash....')
for i,j in zip(dts,dts[1:]):
    print(i,j)
    loopcounter=0
    while True:
        try:
            params = {'create_end_date':j, 'create_start_date':i}
            api_key = 'YOUR API KEY'
            result = get_fresh_query_result('https://redash-id.ninjavan.co/',512, api_key, params)
            resultdf = pd.DataFrame(result)
            print(i,'Pulling Data: Success.')
            print(len(resultdf))
            break
        except:
            print('>> Pulling failed, retrying...')
            # loopcounter = loopcounter +1
            # if loopcounter >=5:
            #     break
            
    dataframes.append(resultdf)

raw_data = pd.concat(dataframes)

# WJ
raw_data = raw_data[raw_data['pets_type'].isin(['MISSING','DAMAGED'])]
raw_data = raw_data[(raw_data['investigating_hub_region'] == 'Greater Jakarta') & (raw_data['current_assignee_group'] == 'Fleet (Last Mile)')
                    & (raw_data['investigating_hub_area'] != '')]
raw_data['refreshed_at'] = datetime.now()
raw_data = raw_data[raw_data['investigating_hub_area'] != '']
print('Total In-Progress PETS Ticket: ', len(raw_data))


# Upload to Google Sheets
print('>> Upload to Google Sheets...')

# Export to Gsheet Func
def export_to_sheets(file_name,sheet_name,df,mode='r'):
    ws = gc.open(file_name).worksheet(sheet_name)
    if(mode=='w'):
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=df,include_index=False,include_column_header=True,resize=True)
        return True
    elif(mode=='a'):
        #ws.add_rows(4)
        old = gd.get_as_dataframe(worksheet=ws)
        updated = pd.concat([old,df])
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=updated,include_index=False,include_column_header=True,resize=True)
        return True
    else:
        return gd.get_as_dataframe(worksheet=ws)

countinject = 0
while True:
    try:
        inject = export_to_sheets("Raw Data LM Dashboard - Greater Jakarta", 'PETS', raw_data, mode='w')
        print('>> Upload to Google Sheet DONE')
        break
    except:
        print('>> Failed. Retrying...')


end = datetime.now()
code_runtime = (end - start).total_seconds()/60
print("Elapsed Time: ", code_runtime, 'minutes')