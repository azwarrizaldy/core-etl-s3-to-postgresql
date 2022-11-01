"""
    author  : azwar8597@gmail.com
    project : Core ETL S3 to PostgreSQL
"""


import pandas as pd #type: ignore
import boto3 #type: ignore
import psycopg2 #type: ignore
from sqlalchemy import create_engine #type: ignore
import os #type: ignore
from dotenv import load_dotenv #type: ignore

class ReadData:

    def insert(self):

        #load function dotenv
        load_dotenv()

        #define env aws access key id
        KEY_ID          = os.environ['key']

        #define env aws secret access key
        ACCESS_KEY      = os.environ['acc']

        #define env region name
        REGION          = os.environ['reg']

        #define env S3 bucket name
        BUCKET          = os.environ['buc']

        #define env url server RDS 
        URL             = os.environ['url']

        #define env user server RDS 
        USER            = os.environ['usr']

        #define env password server RDS 
        PASSWORD        = os.environ['pas']

        #define env port server RDS 
        PORT            = os.environ['por']

        #define env database
        DATABASE        = os.environ['db']

        try:

            #inisiasi connect to S3 AWS
            endpoint = boto3.client(
                            's3',
                            aws_access_key_id = '{}'.format(KEY_ID),
                            aws_secret_access_key = '{}'.format(ACCESS_KEY),
                            region_name = '{}'.format(REGION)
            )

            #print success connect to S3
            print("success connect to S3")
        
        except:
            
            #print cannot connect to S3
            print("cannot connect to S3")
        

        try:

            #read file customer from S3
            data1 = endpoint.get_object(Bucket='{}'.format(BUCKET), Key="data-final-project/customer.csv")
            df1 = pd.read_csv(data1["Body"], index_col=[0])

            #get and read file loan repayment from S3
            data2 = endpoint.get_object(Bucket='{}'.format(BUCKET), Key="data-final-project/loan_repayment.csv")
            df2= pd.read_csv(data2["Body"], index_col=[0])

            #get and read file loan status from S3
            data3 = endpoint.get_object(Bucket='{}'.format(BUCKET), Key="data-final-project/loan_status.csv")
            df3 = pd.read_csv(data3["Body"], index_col=[0])

            #get and read file property from S3
            data4 = endpoint.get_object(Bucket='{}'.format(BUCKET), Key="data-final-project/property.csv")
            df4 = pd.read_csv(data4["Body"], index_col=[0])

            #get and read file source income from S3
            data5 = endpoint.get_object(Bucket='{}'.format(BUCKET), Key="data-final-project/source_income.csv")
            df5 = pd.read_csv(data5["Body"], index_col=[0])

            #print success read file form S3
            print("success read file from S3")
        
        except:

            #print cannot read file form S3
            print("cannot read file from S3")

        try:
            
            #initiation connect to postgresql
            engine = create_engine("postgresql://{USER}:{PASSWORD}@{URL}:{PORT}/{DATABASE}".format(
                                        USER        = USER,
                                        PASSWORD    = PASSWORD,
                                        URL         = URL,
                                        PORT        = PORT,
                                        DATABASE    = DATABASE
                                    )
                                )
        

            #print success connect to postgresql
            print("success connect to postgresql")
        
        except:

            #print cannot connect to postgresql
            print("cannot connect to postgresql")
        
        
        try:

            #insert dataframe to rds postgresql
            df1.to_sql("customer", engine, if_exists='append', index=False)
            df2.to_sql("loan_repayment", engine, if_exists='append', index=False)
            df3.to_sql("loan_status", engine, if_exists='append', index=False)
            df4.to_sql("property", engine, if_exists='append', index=False)
            df5.to_sql("source_income", engine, if_exists='append', index=False)

            #print success insert dataframe to rds postgresql
            print("success insert dataframe to postgresql")
        
        except:

            #print cannot insert dataframe to rds postgresql
            print("cannot insert dataframe to postgresql")
