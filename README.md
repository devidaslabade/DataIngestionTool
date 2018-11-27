# DataIngestionTool

## Step to execute the Module


For help :
python driver.py -h 


For excuting jobs :

python driver.py --job=dataPrepartion.dataIngestion --configLoc=C:\\Users\\sk250102\\Documents\\Teradata\\DIT\\DataIngestionTool\\config\\config.cnf --prcs="prc_PrcId_[0-9].json" --pool=3

### Supported Data Sources
1. Delimited Text files (CSV, TAB etc )
	* ***Delimiter*** : The file type for Text based files can either be "csv" or "delimited" but the delimiter value is mandatory to be added in source file having following syntax for csv :  ***"delimiter":","***
	* ***Infer Schema*** : For text based files schema can be inferred by adding the following json filed in Source files : ***"inferSchema":"true"***
	* ***Header*** : Text based files can also have header information and hence the same can be utilized for column refrence by adding following json filed in Source files :  ***"header":"true"***
2. ORC
3. Parquet
4. Json
5. Hive Table
6. JDBC data sources

### Desination Data source writing modes 
1. ***append*** :Append mode means that when saving to a data source, if data/table already exists, contents are expected to be appended to existing data.
2. ***overwrite*** : Overwrite mode means that when saving to a data source, if data/table already exists, existing data is expected to be overwritten by the current contents.
3. ***ignore*** :Ignore mode means that when saving to a data source, if data already exists, then the current content will not be saved or in other words no change the existing data will take place.
4. ***errorifexists*** : ErrorIfExists mode means that when saving to a data source, if data already exists, an exception is expected to be thrown.

The writing mode can be set in destination detail file by adding the json filed : ***"mode":"overwrite"***

### Compression support for Data sources

1. Delimited Text files (CSV, TAB etc ) : (none, bzip2, gzip, lz4, snappy and deflate).
2. json : (none, bzip2, gzip, lz4, snappy and deflate).
3. orc ; (none, snappy, zlib, and lzo)
4. parquet : (none, uncompressed, snappy, gzip, lzo, brotli, lz4, and zstd)

The compression can be set in destination detail files by adding the json filed : ***"compression":"bzip2"***


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
