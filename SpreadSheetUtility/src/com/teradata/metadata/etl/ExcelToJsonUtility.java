package com.teradata.metadata.etl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy;
import org.apache.poi.ss.usermodel.Sheet;

import com.teradata.metadata.etl.utility.MetaDrivenApplicationConstants;

import net.sf.json.JSONObject;

/**
 * The class takes excel file as input and generates json files based on the tab in sheet.
 * @author ns250044
 *
 */
public class ExcelToJsonUtility implements MetaDrivenApplicationConstants {
	
	private final static Logger LOGGER = Logger.getLogger(ExcelToJsonUtility.class);
	
	/**
	 * Global HashMap object for storing all the field mappings between excel and json.
	 */
	private static final HashMap<String, String> PROPERTY_MAP = new HashMap<String, String>();

	/**
	 * This is the entry point for the class.
	 * @param args String[]
	 */
	public static void main(String[] args) {
		loadProperties();
		String inputFilePath = PROPERTY_MAP.get(INPUT_FILE_PATH);
		createJsonFromExcel(inputFilePath);
	}
	
	/**
	 * This method loads the properties from mapping.properties file into Map object.
	 */
	private static void loadProperties() {
		Properties prop = new Properties();
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream("mapping.properties");
			// load a properties file
			prop.load(inputStream);
			Enumeration keys = prop.propertyNames();
	        while(keys.hasMoreElements()) {
	            String key = (String)keys.nextElement();
	            PROPERTY_MAP.put(key,prop.getProperty(key));
	        }
		} catch (Exception ex) {
			ex.printStackTrace();
			LOGGER.error("Error in loading the mapping properties::"+ex);
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (Exception e) {
					e.printStackTrace();
					LOGGER.error("Error in closing the inputStream::"+e);
				}
			}
		}
		
	}
	
	/**
	 * This method contains business logic for generating the json files from a excel file.
	 * @param inputFilePath String
	 */
	public static void createJsonFromExcel(String inputFilePath) {
        try {
        	/* Open the file. */
			FileInputStream fInputStream = new FileInputStream(inputFilePath.trim());
			HSSFWorkbook excelWorkBook = new HSSFWorkbook(fInputStream);
			// Get all excel sheet count.
            int totalSheetNumber = excelWorkBook.getNumberOfSheets();
            if(PROPERTY_MAP.get(TOTAL_SHEET_COUNT).equals(String.valueOf(totalSheetNumber))) {
            	//Loop in all excel sheet.
            	for(int i=1;i<totalSheetNumber;i++) {
            		Sheet sheet = excelWorkBook.getSheetAt(i);
                    String sheetName = sheet.getSheetName();
                    if(sheetName != null && sheetName.length() > 0)
                    {
                    	String fileNameHeader = PROPERTY_MAP.get(sheetName);
                    	// Get current sheet data in a list table.
                        List<List<String>> sheetDataTable = getSheetDataList(sheet);
                        // Generate JSON format of above sheet data and write to a JSON file.
                        HashMap<String, List<JSONObject>> jsonHolderMap = createJSONAndWriteFromList(sheetDataTable);
                        writeStringToFile(jsonHolderMap, fileNameHeader);
                    } else {
                    	System.out.println("Invalid sheetname");
                    }
            	}
            } else {
            	System.out.println("Number of sheet required for the application is not equal to "+TOTAL_SHEET_COUNT);
            	LOGGER.error("Excel file should have ");
            }
            
            //Close excel work book object. 
            excelWorkBook.close();
			
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("Error in createJsonFromExcel method::"+e);
		}
        
	}
	
	/**
	 * This method reads and return excel sheet data into a java List object. 
	 * @param sheet Sheet
	 * @return List<List<String>>
	 */
    private static List<List<String>> getSheetDataList(Sheet sheet)
    {
        List<List<String>> ret = new ArrayList<List<String>>();

        // Get the first and last sheet row number.
        int firstRowNum = sheet.getFirstRowNum();
        int lastRowNum = sheet.getLastRowNum();

        if(lastRowNum > 0)
        {
            // Loop in sheet rows.
            for(int i=firstRowNum; i<lastRowNum + 1; i++)
            {
                // Get current row object.
                Row row = sheet.getRow(i);

                // Get first and last cell number.
                int firstCellNum = row.getFirstCellNum();
                int lastCellNum = row.getLastCellNum();

                // Create a String list to save column data in a row.
                List<String> rowDataList = new ArrayList<String>();

                // Loop in the row cells.
                for(int j = firstCellNum; j < lastCellNum; j++)
                {
                    // Get current cell.
                    Cell cell = row.getCell(j, MissingCellPolicy.CREATE_NULL_AS_BLANK);
                    
                    // Get cell type.
                    int cellType = cell.getCellType();

                    if(cellType == CellType.NUMERIC.getCode())
                    {
                        //double numberValue = cell.getNumericCellValue();
                    	 int numberValue = (int)cell.getNumericCellValue();

                        // BigDecimal is used to avoid double value is counted use Scientific counting method.
                        // For example the original double variable value is 12345678, but jdk translated the value to 1.2345678E7.
                        //String stringCellValue = BigDecimal.valueOf(numberValue).toPlainString();
                        String stringCellValue = String.valueOf(numberValue);

                        rowDataList.add(stringCellValue);

                    }else if(cellType == CellType.STRING.getCode())
                    {
                        String cellValue = cell.getStringCellValue();
                        rowDataList.add(cellValue);
                    }else if(cellType == CellType.BOOLEAN.getCode())
                    {
                        boolean numberValue = cell.getBooleanCellValue();

                        String stringCellValue = String.valueOf(numberValue);

                        rowDataList.add(stringCellValue);

                    }else if(cellType == CellType.BLANK.getCode())
                    {
                        rowDataList.add("");
                    }
                }//end of for loop

                // Add current row data list in the return list.
                ret.add(rowDataList);
            }
        }
        return ret;
    }
    
    /* Return a JSON string from the string list. */
    /**
     * The method creates a Json data from excel data based on tab. 
     * @param dataTable List<List<String>>
     * @return HashMap<String, List<JSONObject>>
     */
    private static HashMap<String, List<JSONObject>> createJSONAndWriteFromList(List<List<String>> dataTable)
    {
    	HashMap<String, List<JSONObject>> jsonHolderMap = new HashMap<String, List<JSONObject>>();

        if(dataTable != null)
        {
            int rowCount = dataTable.size();

            if(rowCount > 1)
            {
                // Create a JSONObject to store table data.
                //JSONObject tableJsonObject = new JSONObject();

                // The first row is the header row, store each column name.
                List<String> headerRow = dataTable.get(0);

                int columnCount = headerRow.size();

                // Loop in the row data list.
                for(int i=1; i<rowCount; i++)
                {
                	String fileNameSuffix = "";
                	//JSONArray tableJsonArray  = new JSONArray();
                    // Get current row data.
                    List<String> dataRow = dataTable.get(i);

                    // Create a JSONObject object to store row data.
                    JSONObject rowJsonObject = new JSONObject();

                    for(int j=0;j<columnCount;j++)
                    {
                        String columnName = headerRow.get(j);
                        String fieldName = PROPERTY_MAP.get(columnName);
                        if(j >= dataRow.size()) {
                        	continue;
                        }
                        String columnValue = dataRow.get(j);
                        if(columnValue!=null) {
                        	rowJsonObject.put(fieldName, columnValue);
                        } else {
                        	continue;
                        }
                        if(j == 0) {
                        	fileNameSuffix = columnValue;
                        }

                    }
                    List<JSONObject> jsonObjectList = jsonHolderMap.get(fileNameSuffix);
                    if(jsonObjectList != null && jsonObjectList.size() >0) {
                    	jsonObjectList.add(rowJsonObject);
                    	jsonHolderMap.put(fileNameSuffix, jsonObjectList);
                    } else {
                    	jsonObjectList = new ArrayList<JSONObject>();
                    	jsonObjectList.add(rowJsonObject);
                    	jsonHolderMap.put(fileNameSuffix,jsonObjectList);
                    }
                }
            }
        }
        return jsonHolderMap;
    }
    
    /* Write string data to a file.*/
    /**
     * This method generates the json files and put it into output directory.
     * @param jsonMap HashMap<String, List<JSONObject>>
     * @param fileNameHeader String
     */
    private static void writeStringToFile(HashMap<String, List<JSONObject>> jsonMap, String fileNameHeader)
    {
    	//JSONArray tableJsonArray  = new JSONArray();
        try
        {
        	if(jsonMap != null && jsonMap.size() > 0) {
        		for (Map.Entry<String,List<JSONObject>> entry : jsonMap.entrySet()) {
        			String key = entry.getKey();
        			String value = entry.getValue().toString();
        			
        			//tableJsonArray.add(rowJsonObject);
                    String jsonFileName = fileNameHeader + "_" + key + ".json";
                    
                    // Get the output file absolute path.
                    //String filePath = currentWorkingFolder + filePathSeperator + fileName;
                    String filePath = PROPERTY_MAP.get(OUTPUT_FILE_PATH)+jsonFileName;
                    // Create File, FileWriter and BufferedWriter object.
                    File file = new File(filePath);

                    FileWriter fw = new FileWriter(file);

                    BufferedWriter buffWriter = new BufferedWriter(fw);

                    // Write string data to the output file, flush and close the buffered writer object.
                    buffWriter.write(value);

                    buffWriter.flush();

                    buffWriter.close();

                    System.out.println(filePath + " has been created.");
        			
        			
        		}
        	}

        }catch(Exception ex)
        {
            System.err.println(ex.getMessage());
        }
    }

}
