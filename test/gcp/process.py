import sys
import pandas as pd
import argparse
import utils as utl
import common as cmn
import subprocess
import ntpath
from pyspark.sql import SparkSession
sqlcontext=SparkSession.builder.enableHiveSupport().getOrCreate()
sqlcontext.sql("set spark.sql.parquet.writeLegacyFormat true")
__author__ = 'nsandela@cisco.com'

def create_log_file(logfile):
    head, tail = ntpath.split(logfile)
    return tail or ntpath.basename(head)

def main(args):
    """App's main"""
    #start_time = time.time()
    #stime = strftime("%a %b %d %H:%M %Y", gmtime())
    if len(sys.argv)>1:
        with open(args.filename, buffering=-1) as data_file:
            data = data_file.read()
            json_status, json_string = utl.is_json(data)
            if json_status:
                keylist = json_string.keys()
                if utl.validate_input(**json_string):
                        for rules in json_string['RuleList']:
                                my_cls = cmn.GCP()
                                my_cls.process(**rules)
                else:
                    print "Provided json doesn't contain all the necessary keys"

            else:
                print "Provided json is not valid"
    #end_time = time.time ()
    #etime = strftime ('%a %b %d %H:%M %Y', gmtime ())
    #print "Job completed on %s in %s seconds\n" % (etime, time.strftime ("%M:%S", time.gmtime (end_time - start_time)))


if __name__ == '__main__':
    global logger
    parser = argparse.ArgumentParser(description="GCP Processing", epilog="")
    parser.add_argument("filename", help="file with json input")
    cmd_args = parser.parse_args()
    print cmd_args
    main(cmd_args)

