import subprocess
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

sqlcontext = SparkSession.builder.enableHiveSupport().getOrCreate()
sqlcontext.sql("set spark.sql.parquet.writeLegacyFormat true")
projectid = "gvs-cs-cisco"
keyfile = "/hdfs/app/GCS_ANA/google/google-cloud-sdk/GVS-CS-CISCO-4040861fd9bc.json"

__author__ = 'nsandela@cisco.com'


class GCP(object):

    def __init__(self):
        pass

    # bq2hiveTable
    def store_data(self, hiveTable, dataframe):
        """	This function will store the data from dataframe to hadoop table
			This function will take exactly 2 arguments:
			hiveTable- HADOOP Table name
			DataFrame- Spark DataFrame """
        df = sqlcontext.createDataFrame(dataframe)
        df.registerTempTable('gcp_temp')
        sqlcontext.sql("drop table if exists" + " " + hiveTable)
        sqlcontext.sql("create table" + " " + hiveTable + " " + "as select * from gcp_temp")
        print("Successfully Loaded data to" + " " + hiveTable)

    def Dataframe2gcpbucket(self, dataframe, bucketPath='gs://gvscs-sandbox/temp', compress=True, partitions=20):
        """ This Function will write data to a GCP Bucket provided a DataFrame
			This function will take exactly 3 argument:
			dataframe - SparkDataFrame
			bucketPath - GCP Bucket Path
			partitions(Optional) - Number of Partitions"""
        try:
            df = self.datatype_cast_decimal(dataframe=dataframe)
            if compress:
                df = df.coalesce(partitions)
            try:
                print("Writing hiveTable data to " + " " + bucketPath + " " + "in gcp")
                df.write.mode('overwrite').parquet(bucketPath)
            except Exception as e:
                print(str(e))
                raise
        except Exception as e:
            raise

    def hadoop2bq(self, bqTableName, gcp_bucket='gs://gvscs-sandbox/temp', sql_text=None, spark_dataframe=None,
                  hiveTable=None):
        """ This Function will write data to a BigQuery Table provided a DataFrame or hivetable or sql query
			This function will take exactly 5 argument:
            bqTableName - Big Query Table
			dataframe - SparkDataFrame
			gcp_bucket - GCP Bucket Path
            hiveTable - Hive Tablename
			partitions(Optional) - Number of Partitions"""

        if sql_text == None and spark_dataframe == None and hiveTable == None:
            print("Please Provide Enough Arguments to run the Function")
        else:
            if sql_text != None:
                print("Running with Provided SQL Query: " + sql_text)
                # print spark_dataframe
                # print hiveTable
                if spark_dataframe == None and hiveTable == None:
                    self.hadoop2gcpBucket(hiveTable=None, bucketPath=gcp_bucket, statement=sql_text, execute=True)
                else:
                    print("Multiple Arguments referencing multiple operations are provided")
            elif spark_dataframe != None:
                print("Running with Provided Spark Dataframe")
                if sql_text == None and hiveTable == None:
                    self.Dataframe2gcpbucket(spark_dataframe)
                else:
                    print("Multiple Arguments referencing multiple operations are provided")
            elif hiveTable != None:
                print("Running with Provided HiveTable: " + hiveTable)
                if sql_text == None and spark_dataframe == None:
                    self.hadoop2gcpBucket(hiveTable, bucketPath=gcp_bucket)
                else:
                    print("Multiple Arguments referencing multiple operations are provided")
            try:
                self.gcpBucket2bq(bucketPath=gcp_bucket, bqTableName=bqTableName)
                print("Successfully created bqtable from " + " " + gcp_bucket)
                try:
                    self.cleanBucket(gcp_bucket)
                except Exception as e:
                    print(str(e))
                    print("Errored Out while running the cleanBucket Function")
            except Exception as e:
                print(str(e))
                print("Errored Out while running the gcpBucket2bq Function")

    def bq2hiveTable(self, bqTableName):
        """ This function will write the data from BigQuery Table to Hadoop Table
			Effective for tables with little data
			This function will take exactly 2 arguments:
			bqTableName - GCP Table name
			hiveTable - HADOOP Table name """
        df = pd.read_gbq('SELECT * FROM' + " " + bqTableName, project_id=projectid, private_key=keyfile)
        try:
            self.store_data(bqTableName, df)
        except Exception as e:
            print(str(e))

    def bq2bucket(self, bqTableName, bucketPath, format="NEWLINE_DELIMITED_JSON"):
        """  This function will write the data from BigQuery Table to GCP Bucket
			This function will take exactly 3 arguments:
			bqTableName - GCP Table name
			bucketPath - GCP Bucket path
			format(Optional) - File format by default it is JSON"""
        bqStr = 'bq extract --destination_format NEWLINE_DELIMITED_JSON --compression GZIP  \
        {bq_tablename} {gcp_bucketpath}/file-*.json.gz' \
            .format(bq_tablename=bqTableName, gcp_bucketpath=bucketPath)
        print(bqStr)
        try:
            subprocess.check_output(bqStr, shell=True)
        except subprocess.CalledProcessError as e:
            print("Errored Out with error: \n", e.output)
            raise

    def datatype_cast(self, bqtablesdf, schemadf):
        """ This function will cast the DataType from the by looking into the JSON schema exported from GCP
		   This function will take exactly 2 arguments:
	       bqtablesdf - DataFrame Created out of BigQuery Table
	       schemadf - DataFrame of JsonSchema DataFrame"""
        for column in bqtablesdf.columns:
            v = schemadf.index[schemadf['name'] == column].tolist()
            newtype = schemadf.iloc[v[0]]['type']
            if newtype == 'STRING':
                bqtablesdf[column] = bqtablesdf[column].astype(object)
            elif newtype == 'BYTES':
                bqtablesdf[column] = bqtablesdf[column].astype(object)
        return bqtablesdf

    def get_bqschema(self, bqTableName):
        """ This function will get the GCP Table Schema as JSON File
		   This function will take exactly 1 argument:
	       bqTableName - GCP Table Name"""
        global json_path
        json_path = '/hdfs/app/GCS_ANA/gvscsde/projects/gcp/gcp_test/{bqTableName}.json' \
            .format(bqTableName=bqTableName)
        bqStr = 'bq show --schema --format=prettyjson gvs-cs-cisco:{bqTableName} >> {json_path}'.format(
            bqTableName=bqTableName, json_path=json_path)
        print(bqStr)
        try:
            subprocess.check_output(bqStr, shell=True)
        except subprocess.CalledProcessError as e:
            print("Errored Out with Error: \n", e.output)
            raise

    # gcp_hadoop2gcpBucket

    def hadoop2gcpBucket(self, hiveTable, bucketPath, statement='', execute=False, compress=True, partitions=20):
        """ This Function will Export the HADOOP Table and write it to a GCP Bucket
			This function will take exactly 3 argument:
			hiveTable - HADOOP Table Name
			bucketPath - GCP Bucket Path
			partitions(Optional) - Number of Partitions"""
        # print execute
        if execute:
            sqlStr = statement
            print("Running query {statement}".format(statement=sqlStr))
        else:
            print("Retrieving data from hiveTable: " + " " + hiveTable)
            sqlStr = "select * from {}".format(hiveTable)
        try:
            df = sqlcontext.sql(sqlStr)
            df = self.datatype_cast_decimal(df)
            print("Created dataframe with records: " + str(df.count()))
            if compress:
                df = df.coalesce(partitions)
            try:
                print("Writing hiveTable data to " + " " + bucketPath + " " + "in gcp")
                df.write.mode('overwrite').parquet(bucketPath)
            except Exception as e:
                print(str(e))
                raise
        except Exception as e:
            raise

    def gcpBucket2bq(self, bucketPath, bqTableName, tableFormat="PARQUET"):
        """ This will write data from GCP Bucket to BigQuery Table
			This function will take exactly 3 arguments:
			bqTableName - BigQuery Table Name
			bucketPath - GCP Bucket Path
			tableFormat(Optional) - Default is Parquet
		"""
        print("Loading data from " + bucketPath + "to" + bqTableName + " " + "in gcp")
        bqStr = 'bq load  --replace=true --source_format={table_format}  {bq_tablename} {gcp_bucketpath}' \
            .format(table_format=tableFormat, bq_tablename=bqTableName, gcp_bucketpath=bucketPath + "/*.parquet")
        print(bqStr)
        try:
            subprocess.check_output(bqStr, shell=True)
        except subprocess.CalledProcessError as e:
            print("Errored Out with Error: \n", e.output)
            raise

    def cleanBucket(self, bucketPath):
        """ This function will clean the data from GCP Bucket
			This function will take exactly 1 argument:
			bucketPath - GCP Bucket Path
		"""
        rmStr = 'gsutil -m rm -r {}'.format(bucketPath + "/*.parquet")
        try:
            subprocess.check_output(rmStr, shell=True)
            print("Successfully Deleted data from " + bucketPath + " " + "in gcp")
        except subprocess.CalledProcessError as e:
            print("Errored Out with Error: \n", e.output)
            raise

    def apply_function(self, df, fields, function):
        """ This function will apply a function to provided fields in a DataFrame
			This function will take exactly 3 arguments:
			df - DataFrame
			fields - DataFrame Fields
			function - Function name
		"""
        for field in fields:
            df = df.withColumn(field, function(field))
        return df

    def decimal_to_bigint(self, column):
        """ This function will cast the given column to decimal datatype to bigint
			This function will take exactly 1 arguments:
			column - DataFrame Field name
		"""
        return F.col(column).cast(LongType()).alias(column)

    def datatype_cast_decimal(self, dataframe):
        """ This function will cast the decimal datatype to bigint
			This function will take exactly 1 arguments:
			dataframe - DataFrame name
		"""
        df_list = dict(dataframe.dtypes)
        # print df_list
        lst = []
        for column in dataframe.columns:
            if 'decimal(' in df_list.get(column):
                lst.append(column)
        if len(lst) > 0:
            print("Changing the datatype for following columns: " + str(lst))
            df = self.apply_function(dataframe, lst, self.decimal_to_bigint)
        else:
            df = self.apply_function(dataframe, lst, self.decimal_to_bigint)
        return df

    def process(self, **rules):
        """ This function will execute the Pipeline from HADOOP to GCP & Vice Versa
			This Function takes 5 Arguments
			**rules - JSON File, hiveTable - HADOOP Table Name, bqTableName - GCP TableName,
			bucketPath - GCP Bucket Path, process_name - Which Process to implement[bq2hiveTable or hadoop2gcpBucket]
		"""
        print("Executing RuleNumber: " + str(rules['RuleNumber']))
        hiveTable = rules['hiveTable']
        bqTableName = rules['bqTableName']
        bucketPath = rules['bucketPath']
        process_name = rules['Process']
        if process_name == 'bq2hiveTable':
            try:
                self.bq2bucket(bqTableName, bucketPath)
                print("Successfully Loaded data from bqTable to gcpbucket")
                try:
                    df = sqlcontext.read.json(bucketPath + "/file-*.json.gz")
                    pdsdf = df.toPandas()
                    delete_json_file = 'rm -f' + " " + json_path
                    subprocess.check_call(delete_json_file, shell=True)
                    self.get_bqschema(bqTableName)
                    df = pd.read_json(json_path)
                    print("Successfully read the json data to dataframe")
                    self.datatype_cast(pdsdf, df)
                    print("Successfully Casted the datatypes")
                    self.store_data(pdsdf)
                    print("Successfully loaded the dataframe to hiveTable")
                except Exception as e:
                    print(str(e))
            except Exception as e:
                print(str(e))
        elif process_name == 'hadoop2gcpBucket':
            try:
                # print ("Started the process")
                if 'sqlText' in rules.keys():
                    # global statement
                    statement = rules['sqlText']
                    self.hadoop2gcpBucket(hiveTable, bucketPath, statement=statement, execute=True)
                    print("Successfully wrote data to " + " " + bucketPath)
                else:
                    print("Running without any statement")
                    self.hadoop2gcpBucket(hiveTable, bucketPath)
                    print("Successfully wrote data to " + " " + bucketPath)
                try:
                    self.gcpBucket2bq(bucketPath, bqTableName)
                    print("Successfully created bqtable from " + " " + bucketPath)
                    try:
                        self.cleanBucket(bucketPath)
                    except Exception as e:
                        print(str(e))
                        print("Errored Out while running the cleanBucket Function")
                except Exception as e:
                    print(str(e))
                    print("Errored Out while running the gcpBucket2bq Function")
            except Exception as e:
                print(str(e.__str__()))
                print("Errored Out while running the hadoop2gcpBucket Function")
                raise
        else:
            print("Operation not permitted please refer to properties file for valid operations")
