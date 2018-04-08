
from google.cloud import bigquery

import config_1 as config
import os
import time
from datetime import datetime
from threading import Thread, Lock
import logging

#Add DB connection config.py
x =0 
warray = []
result_queue = []
class DatabaseWorker(Thread):
    #__lock = Lock()

    def __init__(self, query, result_queue):
        Thread.__init__(self)
        self.db = "Bigquery"
        self.qry = query
        self.result_queue = result_queue

    def run(self):
        result = None
        global x 
        logging.info("Connecting to database...")
        try:
          
            
            client = bigquery.Client()
            query_job = client.query(self.qry )
            x = x + 1
            print "x : %d" %x
            print"--"
            result =  query_job.result()  
            #print result 
            dt = datetime.now()
            print "date %s" %dt
            
           
               
            
            
        except Exception as e:
            logging.error("Unable to access database %s" % str(e))
        self.result_queue.append(1)

delay = 1

placeholder = [<val1>,<val2>,<val3>,...] 

## You can add all query here in sameformat
#1
for i in placeholder:
    #print i
    
    pq = """ select organization_id, sum(sales_amount), sum(total_enters) , sum(total_exits), sum(number_of_transactions), 
    sum(labor_hours)
    from STAn.site_daily_agg where organization_id = %s
    and  transaction_date >= <date1>
    and transaction_date < <date2> group by organization_id """ %i
    
    c = DatabaseWorker( pq,        result_queue)
    warray.append(c)
# 2 org   for  month
for i in placeholder:
    #print i
    
    ae = """ select organization_id, sum(sales_amount), sum(total_enters) , sum(total_exits), sum(number_of_transactions), 
    sum(labor_hours)
    from  STAn.site_daily_agg where organization_id = %s
    and  transaction_date >= <date1>
    and transaction_date < <date2> group by organization_id """ %i
    #print pq
    c7 = DatabaseWorker( ae,        result_queue)
    warray.append(c7)
    
#3 org ,site for  year
for i in placeholder:
    #print i
   
    q = """ select organization_id, site_id, sum(sales_amount), sum(total_enters) , sum(total_exits), sum(number_of_transactions), 
    sum(labor_hours)
    from  STAn.site_daily_agg where organization_id = %s
    and  transaction_date >= <date1>
    and transaction_date < <date2> group by organization_id, site_id """ %i
    
    c1 = DatabaseWorker( q,        result_queue)
    warray.append(c1)
#4 org  , site for  moth
for i in placeholder:
    #print i
   
    af = """ select organization_id, site_id, sum(sales_amount), sum(total_enters) , sum(total_exits), sum(number_of_transactions), 
    sum(labor_hours)
    from  STAn.site_daily_agg where organization_id = %s
    and  transaction_date >= <date1>
    and transaction_date < <date2> group by organization_id, site_id """ %i
    
    c8 = DatabaseWorker( af,        result_queue)
    warray.append(c8)

    
num_itration = (number of queries * number of parameter in placeholder) - 1
for i in range(0,num_itration):
    warray[i].start()
    
    
    


# Wait for the job to be done

while len(result_queue) < num_itration + 1:
    print result_queue
    print "threading %d" %len(result_queue)
   time.sleep(delay)
    
job_done = True
print job_done
