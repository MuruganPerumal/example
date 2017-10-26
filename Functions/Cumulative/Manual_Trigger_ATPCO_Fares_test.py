'''
Update Manual trigger collection lowest fare and carrier for each dep_date and OD+compartment combination 

'''

from collections import defaultdict
from copy import deepcopy
import pymongo
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from dateutil.parser import parse
import global_variable as var
import timeit



# Connect mongodb db business layer
try:
    conn=pymongo.MongoClient(var.mongo_client_url)[var.database]
    conn.authenticate('dbteam', 'KNjSZmiaNUGLmS0Bv2', source='admin')

except Exception as e:
    #sys.stderr.write("Could not connect to MongoDB: %s" % e)
    print("Could not connect to MongoDB: %s" % e)
start_time = timeit.default_timer()


try:
    # sale create match fields for now added in hadhoc doc aftersome time later we need to change
    manual_dep_od_comp_pipe =[
            #trx data should be system date it has to run based on that
            {'$match': {'trx_date': '2017-04-13'}},
            {'$unwind': '$dep_date'},
            {'$group':{
                    '_id':{
                                    'dep_date':'$dep_date',
                                    'compartment':'$compartment.compartment',
                                    'od':'$od'
                            }
                    }
            },
            {'$project':{
                    'dep_date':'$_id.dep_date',
                    'compartment':'$_id.compartment',
                    'od':'$_id.od'
            }},
            {
                    '$out': 'Temp_manual_dep_od_comp_pipe'
            }
    ]

    # manual_od_comp_pipe pipe creation for the unique combination od do+compartment for querying to INFARE collection
    manual_od_comp_pipe =[
            # trx data should be system date it has to run based on that
            {'$match': {'trx_date': '2017-04-13'}},
            {'$unwind': '$dep_date'},
            {'$group':{
                    '_id':{
                                    'compartment':'$compartment.compartment',
                                    'od':'$od'
                            }
                    }
            },
            {'$project':{
                    'compartment':'$_id.compartment',
                    'od':'$_id.od'
            }},
            {
                    '$out': 'Temp_manual_od_comp_pipe'
            }
    ]
    # running the aggregate pipeline from Manual trigger collection
    conn.JUP_DB_Manual_Triggers_Module.aggregate(manual_dep_od_comp_pipe, allowDiskUse=True)

    # running the aggregate pipeline from Manual trigger collection
    conn.JUP_DB_Manual_Triggers_Module.aggregate(manual_od_comp_pipe , allowDiskUse=True)

    # download the result from both temp collection
    manual_dep_od_comp = conn.Temp_manual_dep_od_comp_pipe.find({},{'_id':0})
    manual_od_comp = conn.Temp_manual_od_comp_pipe.find({},{'_id':0})
    manual_dep_od_comp_each_list = list()
    for manual_dep_od_comp_each in manual_dep_od_comp:
        manual_dep_od_comp_each_list.append(manual_dep_od_comp_each)
    manual_od_comp_list = list()
    for manual_od_comp_each in manual_od_comp:
        manual_od_comp_list.append(manual_od_comp_each)
    print(len(manual_od_comp_list),len(manual_dep_od_comp_each_list))
    odCount = 0
    odDepCount = 0
    elapsed = 0
    
    for manual_od_comp_each in manual_od_comp_list:
        #get the list of dates are coming in the relevent market combination
        curr_comb_date = list()
        for manual_dep_od_comp_each in manual_dep_od_comp_each_list:
            if manual_dep_od_comp_each['compartment'] == manual_od_comp_each['compartment'] and manual_dep_od_comp_each['od'] == manual_od_comp_each['od']:
                curr_comb_date.append(manual_dep_od_comp_each['dep_date'])
        # build the query builder for download the market data from Infare collection
        qry_pi = dict()
        qry_pi['od'] = {'$eq': manual_od_comp_each['od']}
        qry_pi['compartment'] = {'$eq': manual_od_comp_each['compartment']}
        
        #projecting fields
        proj_pi = dict()
        proj_pi['outbound_departure_date'] = 1
        proj_pi['inbound_departure_date'] = 1
        proj_pi['price_inc'] = 1
        proj_pi['carrier'] = 1
        #proj_pi['_id'] = 0
        #download infare from table data based on the market
        infareColl = conn.JUP_DB_Infare.find(qry_pi,proj_pi).limit(5)

        # create a dictionary which should have tuple as key and one value

        # get the data from infareColl cursor
        for infareColl_each in infareColl:
            #load the departure dates relevent for the relevent market
            for curr_comb_date_each in curr_comb_date:
                if (curr_comb_date_each <= infareColl_each['outbound_departure_date'] and curr_comb_date_each >= infareColl_each['inbound_departure_date']):
                    print(infareColl_each)
                    print(curr_comb_date_each)
        elapsed = timeit.default_timer() - start_time
        if odCount% 100 == 0:
            print(str(odCount),' / ',len(manual_od_comp_list))
            print('timer   ', str(elapsed))

        #print(curr_comb_date)
        odCount+=1
    print(str(odCount), ' / ', len(manual_od_comp_list))
    print('timer   ', str(elapsed))
        
except Exception as e:
    print e
