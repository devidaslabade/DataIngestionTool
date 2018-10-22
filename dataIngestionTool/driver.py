import sys
import importlib
import argparse
import time



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='_____________________DataIngestionTool Execution utility_____________________')
    parser.add_argument('--job','-j', type=str, required=True, dest='job_name', help="The name of the job module you want to run. (ex: dataPrepartion.dataIngestion will run dataIngestion.py job of dataPrepartion package)")
    parser.add_argument('--config','-c', type=str, required=True, dest='config', help="Absolute path of configuration file required to run the job ")
    args = parser.parse_args()
    print ("_____________________DataIngestionTool Execution utility_____________________")
    print("Executing with following arguments\n %s" %args)
    start = time.time()
    try:
        module = importlib.import_module(args.job_name)
        module.main(args.config,args)
        end = time.time()
        print ("\nExecution of job %s took %s seconds" % (args.job_name, end-start))
    except Exception as e:
         print (str(datetime.datetime.now()) + "____________ Abruptly Exited________________")
         raise Exception("Exception::Job %s failed with msg %s" %(args.job_name, str(e)))   
            

