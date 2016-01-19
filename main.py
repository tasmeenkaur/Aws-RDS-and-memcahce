import MySQLdb
import boto
import sys
import csv
import time
import urllib2
from boto.s3.key import Key
from boto.s3.connection import S3Connection
import hashlib
import memcache
from boto.s3.key import Key
from boto.s3.connection import S3Connection

def upload_file_to_s3():
    AWS_ACCESS_KEY_ID='AKIAJ3U5SM4YYULXU3UA'
    AWS_SECRET_ACCESS_KEY='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'
    con=S3Connection(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,validate_certs=False,is_secure=False)
    bucket_name=con.create_bucket('tasmeenbuck9189')
    k= Key(bucket_name)
    k.key= 'data1.csv'
    start = time.time()
    k.set_contents_from_filename('data1.csv')
    
    end = time.time()
    total_time = end-start
    
    print(total_time) 
    con.close()
    
def load_data():
    conn = MySQLdb.connect(host= "tasmeen91.cqq8oztrleij.us-west-2.rds.amazonaws.com",
                  user="root",
                  passwd="tasmeen91",
                  db="all_month")
    
    url = 'https://s3.amazonaws.com/tasmeenbuck9189/data1.csv'
    response = urllib2.urlopen(url)
    cr = csv.reader(response)

    cursor = conn.cursor()
    start_time=time.clock()
    cursor.execute("drop table data")
    cursor.execute("create table IF NOT EXISTS data(DRG_Definition varchar(255),Provider_Id varchar(255),Provider_Name varchar(255),Address varchar(255),City varchar(255), State varchar(255),Zip varchar(255),Region varchar(255), Total_discharge varchar(255), Average_Covered_Charges varchar(255), Average_Total_Payments varchar(255), Average_Medicare_Payments varchar(255))")
    count = 0
    for row in cr:
        cursor.execute("INSERT INTO data(DRG_Definition,Provider_Id,Provider_Name,Address,City,State,Zip,Region,Total_discharge,Average_Covered_Charges,Average_Total_Payments,Average_Medicare_Payments) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row)
        print count
        count+=1
        
        #data = csv.reader(open('https://s3.amazonaws.com/tasmeen/all_month.csv',"rb"))
#csv.reader(open('https://s3.amazonaws.com/tasmeen/all_month.csv',"rb"))
#print data
    end_time= time.clock()
    total_time=end_time-start_time
    print total_time
    conn.commit()
    
#cursor.execute("create table if not exists all_month(time varchar(225), latitude varchar(225),longitude varchar(225), depth varchar(225), mag varchar(225),magType varchar(225), nst varchar(225), gap varchar(225), dmin varchar(225),rms varchar(225), net varchar(225), id varchar(225) PRIMARY KEY, updated varchar(225), place varchar(225), type varchar(225))")
#for records in data:
#   cursor.execute("INSERT INTO all_month(time,latitude,longitude,depth,mag,magType,nst,gap,dmin,rms,net,id,updated,place,type) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (records,))


    conn.close()
    
def queries():
    conn = MySQLdb.connect(host= "tasmeen91.cqq8oztrleij.us-west-2.rds.amazonaws.com",
                  user="root",
                  passwd="tasmeen91",
                  db="all_month")
    cursor = conn.cursor()
    s_time=time.clock()
    for a in range(1,1001):
        cursor.execute("select Provider_Name from data ORDER BY RAND() LIMIT 1 ")
    e_time = time.clock()
    t_time = e_time - s_time
    print "time taken to execute 1000 queries: "
    print t_time
    s1_time=time.clock()
    for a1 in range(1,5001):
        cursor.execute("select Provider_Name from data ORDER BY RAND() LIMIT 1 ")
    e1_time = time.clock()
    t1_time = e1_time - s1_time
    print "time taken to execute 5000 queries: "
    print t1_time
    s2_time=time.clock()
    for a2 in range(1,20001):
        cursor.execute("select Provider_Name from data ORDER BY RAND() LIMIT 1")
    e2_time = time.clock()
    t2_time = e2_time - s2_time
    print "time taken to execute 20000 queries: "
    print t2_time

    conn.commit()
    conn.close()
def tuple_queries():
    conn = MySQLdb.connect(host= "tasmeen91.cqq8oztrleij.us-west-2.rds.amazonaws.com",
                  user="root",
                  passwd="tasmeen91",
                  db="all_month")
    cursor = conn.cursor()
    cursor.execute("select Provider_Name from data ORDER BY RAND() LIMIT 199,600")
    record = cursor.fetchall()
    start_time = time.clock()
    for row in record:
        for a in range (1,1001):
            cursor.execute("select Provider_Name from data where Provider_Name = %s ORDER BY RAND()", row)
    end_time = time.clock()
    total_time = end_time - start_time
    print ("time taken to run ")
    print total_time
    start_time1 = time.clock()
    for row in record:
        for a in range (1,5001):
            cursor.execute("select Provider_Name from data where Provider_Name = %s ORDER BY RAND()", row)
    end_time1 = time.clock()
    total_time1 = end_time1 - start_time1
    print ("time taken to run ")
    print total_time1
    start_time2 = time.clock()
    for row in record:
        for a in range (1,20001):
            cursor.execute("select Provider_Name from data where Provider_Name = %s ORDER BY RAND()", row)
    end_time2 = time.clock()
    total_time2 = end_time2 - start_time2
    print ("time taken to run ")
    print total_time2
    
memClient = memcache.Client(['tas.qkrhb4.0001.usw2.cache.amazonaws.com:11211'],debug=0)
def memcache():

    db = MySQLdb.connect(host= "tasmeen91.cqq8oztrleij.us-west-2.rds.amazonaws.com",user="root",passwd="tasmeen91",db="all_month")
    
    
	
    query = "select Provider_Id from data"
    hash_key = hashlib.md5()
    hash_key.update(query)
    key = hash_key.hexdigest()
    
  
    cursor = db.cursor()
    start_time = time.clock()
    if memClient.get(key):
        print " Got data"
       
    else:
       cursor.execute('select Provider_Id from data')
       rows = cursor.fetchall()
       memClient.set(key,rows)
       print "Not found"
   
    end_time = time.clock()
    total_time = end_time - start_time
    print " Total time taken by memcache :"
    print(total_time)
 

    
    
def main(argv):
    
    while (True):
        print "menu \n"
        print "1.upload file to s3. 2 . load data  3. random queries 4. tuple queries 5. memcache 6. EXIT "
        option = raw_input("enter option")
        if (option == '1'):
            upload_file_to_s3()
        elif(option == '2'):
            load_data()
        elif (option == '3'):
            queries()
        elif(option == '4'):
            tuple_queries()
        elif(option == '5'):
            memcache()
        elif(option== '6'):
            sys.exit(0)

        
if __name__ == '__main__':
  main(sys.argv)
