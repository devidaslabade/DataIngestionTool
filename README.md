# DataIngestionTool

## Step to execute the Module


For help :
python driver.py -h 


For excuting jobs :

python driver.py --job=dataPrepartion.dataIngestion --configLoc=C:\\Users\\sk250102\\Documents\\Teradata\\DIT\\DataIngestionTool\\config\\config.cnf --prcs="prc_PrcId_[0-9].json" --pool=3



### Supported datatypes 

*Datatypes are not case sensitive*

	INT 		Integer/Int  (a signed 32-bit integer)
	Long		LongInteger/Long (a signed 64-bit integer)
	FLOAT		SingleReal/Float
	DOUBLE		Real/Double
	BOOLEAN     Boolean/Bool
	String		String
	Timestamp	DateTime (Format : yyyy-MM-dd HH:mm:ss)
	
*Datatypes such as Timestamp should match else rows will be null*	
	


### Default values of entities
Make sure that there is no col mapping and use follwing entry for destcols Json file \
 For setting default string literal :
 
	Json element syntax : "default":"123"	
	
 For setting default SQL functions 	
	
	Json element syntax : "default":"from_unixtime(unix_timestamp(), 'yy-MM-dd hh:mm:ssZ')"



### Supported SQL functions 

( https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF )

#### String Functions

	1. concat(string|binary A, string|binary B...) :
		Json element syntax : "transFunc":"concat({0} ,'string_to_be_concatenated',...)"
	2. concat_ws(string SEP, string A, string B...) :
		Json element syntax : "transFunc":"concat_ws('SEPERATOR',{0} ,'string_to_be_concatenated',...)" 
	3. encode(string src, string charset) :
		Json element syntax : "transFunc":"encode({0} ,'UTF-8')"
	4. format_number(number x, int d)
		Json element syntax : "transFunc":"encode({0} ,'UTF-8')"  --> To check
	5. length(string A)
		Json element syntax : "transFunc":"length({0} )"
	6. lower(string A)
		Json element syntax : "transFunc":"lower({0} )"
	7. lpad(string str, int len, string pad)
		Json element syntax : "transFunc":"lpad({0} ,3,'#')"
	8. repeat(string str, int n)
		Json element syntax : "transFunc":"repeat({0} ,3)"
	9. replace(string A, string OLD, string NEW)
		Json element syntax : "transFunc":"encode({0} ,'UTF-8')"
		--> pyspark.sql.utils.AnalysisException: "Undefined function: 'replace'. This function is neither a registered temporary function nor a permanent function registered in the database 'default'.; line 1 pos 5"

	10. reverse(string A)
		Json element syntax : "transFunc":"reverse({0})"
	11. substr(string|binary A, int start) / substr(string|binary A, int start, int len) 
		Json element syntax : "transFunc":"substr({0},2,4)"
	12. substring(string|binary A, int start) / substring(string|binary A, int start, int len)
		Json element syntax : "transFunc":"substring({0},2,4)"
	13. trim(string A)
		Json element syntax : "transFunc":"trim({0})"
	14. upper(string A) 
		Json element syntax : "transFunc":"upper({0})"
	16. ucase(string A)
		Json element syntax : "transFunc":"ucase({0})"

#### Conditional Functions
	1. nvl(T value, T default_value)
		Json element syntax : "transFunc":"nvl({0},'replacement_string')"
	2. COALESCE(T v1, T v2, ...)
		Json element syntax : "transFunc":"COALESCE({0},CAST('replacement_string' as string))"

	
#### Data Masking Functions
	1. mask(string str[, string upper[, string lower[, string number]]])
		Json element syntax : "transFunc":"mask({0},'U','L','#')"
	2. mask_first_n(string str[, int n])
		Json element syntax : "transFunc":"encode({0} ,'UTF-8')"
	3. mask_last_n(string str[, int n])
		Json element syntax : "transFunc":"encode({0} ,'UTF-8')"
	4. mask_show_first_n(string str[, int n])
		Json element syntax : "transFunc":"encode({0} ,'UTF-8')"
	5. mask_show_last_n(string str[, int n])
		Json element syntax : "transFunc":"encode({0} ,'UTF-8')"
		
pyspark.sql.utils.AnalysisException: "Undefined function: 'mask'. This function is neither a registered temporary function nor a permanent function registered in the database 'default'.; line 1 pos 5"

#### Data Encryption
	1. aes_encrypt(input string/binary, key string/binary)
		Json element syntax : "transFunc":"base64(aes_encrypt({0}, '1234567890123456'))"
	2. aes_decrypt(input binary, key string/binary)
		Json element syntax : "transFunc":"encode({0} ,'UTF-8')"
pyspark.sql.utils.AnalysisException: "Undefined function: 'aes_encrypt'. This function is neither a registered temporary function nor a permanent function registered in the database 'default'.; line 1 pos 12"

#### Date Functions
	1. from_unixtime(bigint unixtime[, string format])
		Json element syntax : "transFunc":"from_unixtime({0} ,'yyyy/MM/dd')"
	2. unix_timestamp()
		Json element syntax : "transFunc":"unix_timestamp()"
		
	3. unix_timestamp(string date)
		Json element syntax : "transFunc":"unix_timestamp({0})"
	4. unix_timestamp(string date, string pattern)
		Json element syntax : "transFunc":"unix_timestamp({0}, 'yyyy-dd-MM HH:mm:ss')"
	5. to_date(string timestamp)
		Json element syntax : "transFunc":"to_date(current_timestamp())"
	6. current_date
		Json element syntax : "transFunc":"current_date()"
	7. current_timestamp
		Json element syntax : "transFunc":"current_timestamp()"

		
		
***Note:* To convert a date string of one format to another use following Json element directly in destCols**

Json element syntax : "transFunc":"from_unixtime(unix_timestamp({0}, 'yyyy-dd-MM HH:mm:ss'),'yyyy/MM/dd')"

#### Mathematical Functions
	1. round(DOUBLE a)
		Json element syntax : "transFunc":"round({0})"
	2. round(DOUBLE a, INT d)
		Json element syntax : "transFunc":"round({0})"
	3. floor(DOUBLE a)
		Json element syntax : "transFunc":"floor({0})"
	4. ceil(DOUBLE a)
		Json element syntax : "transFunc":"ceil({0})"
	5. ceiling(DOUBLE a)
		Json element syntax : "transFunc":"ceiling({0})"
