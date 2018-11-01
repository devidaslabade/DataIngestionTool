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

( https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF )

#### String Functions

	concat(string|binary A, string|binary B...)
	concat_ws(string SEP, string A, string B...)
	encode(string src, string charset)
	format_number(number x, int d)
	length(string A)
	lower(string A)
	lpad(string str, int len, string pad)
	repeat(string str, int n)
	replace(string A, string OLD, string NEW)
	reverse(string A)
	substr(string|binary A, int start) / substr(string|binary A, int start, int len) 
	substring(string|binary A, int start) / substring(string|binary A, int start, int len)
	trim(string A)
	upper(string A) 
	ucase(string A)

#### Conditional Functions
	nvl(T value, T default_value)
	COALESCE(T v1, T v2, ...)

	
#### Data Masking Functions
	mask(string str[, string upper[, string lower[, string number]]])
	mask_first_n(string str[, int n])
	mask_last_n(string str[, int n])
	mask_show_first_n(string str[, int n])
	mask_show_last_n(string str[, int n])

#### Data Encryption
	aes_encrypt(input string/binary, key string/binary)
	aes_decrypt(input binary, key string/binary)

#### Date Functions
	from_unixtime(bigint unixtime[, string format])
	unix_timestamp()
	unix_timestamp(string date)
	unix_timestamp(string date, string pattern)
	to_date(string timestamp)
	current_date
	current_timestamp
	date_format(date/timestamp/string ts, string fmt)

#### Mathematical Functions
	round(DOUBLE a)
	round(DOUBLE a, INT d)
	floor(DOUBLE a)
	ceil(DOUBLE a)
	ceiling(DOUBLE a)
