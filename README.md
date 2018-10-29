# DataIngestionTool

## Step to execute the Module


For help :
python driver.py -h 


For excuting jobs :

python driver.py --job=dataPrepartion.dataIngestion --configLoc=C:\\Users\\sk250102\\Documents\\Teradata\\DIT\\DataIngestionTool\\config\\config.cnf --prcs="prc_PrcId_[0-9].json" --pool=3



### Supported datatypes 

( https://docs.tibco.com/pub/sfire-analyst/7.7.1/doc/html/en-US/TIB_sfire-analyst_UsersGuide/connectors/apache-spark/apache_spark_data_types.htm ) :


INT 		Integer/Int
BIGINT		LongInteger/Long
FLOAT		SingleReal/Float
DOUBLE		Real/Double
BOOLEAN     Boolean/Bool
STRING		String
TIMESTAMP	DateTime


### Supported SQL functions 




code_path = glob.glob(os.path.join(location, '*.tsol'))
	example = glob.glob(os.path.join(location, '*.json'))
len(glob.glob(feature_dir + BOX_FEATURE + '*.npy'))
for lport in glob.glob("/sys/class/net/%s/lower_*" % port):
pattern = self.distribution.get_name() + '*' + pattern
            files = glob(os.path.join(self.dist_dir, pattern))