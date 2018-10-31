import sys
import importlib
import argparse
import time



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='_____________________DataIngestionTool Execution utility_____________________')
    parser.add_argument('--job','-j', type=str, required=True, dest='job_name', help="The name of the job module you want to run. (ex: dataPrepartion.dataIngestion will run dataIngestion.py job of dataPrepartion package)")
    parser.add_argument('--configLoc','-c', type=str, required=True, dest='config', help="Absolute path of configuration file required to run the job ")
    parser.add_argument('--prcs','-p',type=str, required=True, dest='prcs', help="Regex Expression for fetching specific files ex: (prc__PrcId_(1|21|12)|prc_(PrcId_[4-6])).json -> will fetch files having ids 1,21,12,4,5,6")
    parser.add_argument('--pool',type=int, required=True, dest='pool', help="Parallel thread pool for executing simultaneous Spark DAGs")
    args = parser.parse_args()
    print ("_____________________DataIngestionTool Execution utility_____________________")
    print("Executing with following arguments\n %s" %args)
    start = time.time()
    try:
        module = importlib.import_module(args.job_name)
        module.main(args.config,args.prcs,args.pool)
        end = time.time()
        print ("\nExecution of job %s took %s seconds" % (args.job_name, end-start))
    except Exception as e:
         print (str(datetime.datetime.now()) + "____________ Abruptly Exited________________")
         raise Exception("Exception::Job %s failed with msg %s" %(args.job_name, str(e)))   
            

