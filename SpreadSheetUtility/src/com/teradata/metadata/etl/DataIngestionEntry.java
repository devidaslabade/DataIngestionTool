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
import java.util.Properties;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import com.teradata.metadata.etl.utility.MetaDrivenApplicationConstants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class DataIngestionEntry {
	
	private static final HashMap<String, String> MAPPING_MAP = new HashMap<String, String>();

	public static void main(String[] args) {
		loadProperties();
		String inputFilePath = "C:\\Users\\ns250044\\Documents\\Project\\Utility\\inputfile\\utilityinputdetails.xls";
		createJsonFromExcel(inputFilePath);
	}
	
	private static void loadProperties() {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream("mapping.properties");
			// load a properties file
			prop.load(input);
			Enumeration keys = prop.propertyNames();
	        while(keys.hasMoreElements()) {
	            String key = (String)keys.nextElement();
	            MAPPING_MAP.put(key,prop.getProperty(key));
	        }
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	public static void createJsonFromExcel(String inputFilePath) {
        try {
        	/* Open the file. */
			FileInputStream fInputStream = new FileInputStream(inputFilePath.trim());
			HSSFWorkbook excelWorkBook = new HSSFWorkbook(fInputStream);
			// Get all excel sheet count.
            int totalSheetNumber = excelWorkBook.getNumberOfSheets();
            /*if(totalSheetNumber != 4) {
            	return;
            }*/
            //Loop in all excel sheet.
            
            Sheet sheet = excelWorkBook.getSheetAt(0);
            String sheetName = sheet.getSheetName();
            if(sheetName != null && sheetName.length() > 0)
            {
            	String fileNameHeader = MAPPING_MAP.get(sheetName);
            	// Get current sheet data in a list table.
                List<List<String>> sheetDataTable = getSheetDataList(sheet);
                // Generate JSON format of above sheet data and write to a JSON file.
                createJSONAndWriteFromList(sheetDataTable, fileNameHeader);
            }
            //Close excel work book object. 
            excelWorkBook.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
        
	}
	
	/* Return sheet data in a two dimensional list. 
     * Each element in the outer list is represent a row, 
     * each element in the inner list represent a column.
     * The first row is the column name row.*/
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
                    Cell cell = row.getCell(j);

                    // Get cell type.
                    int cellType = cell.getCellType();

                    if(cellType == CellType.NUMERIC.getCode())
                    {
                        double numberValue = cell.getNumericCellValue();

                        // BigDecimal is used to avoid double value is counted use Scientific counting method.
                        // For example the original double variable value is 12345678, but jdk translated the value to 1.2345678E7.
                        String stringCellValue = BigDecimal.valueOf(numberValue).toPlainString();

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
                }

                // Add current row data list in the return list.
                ret.add(rowDataList);
            }
        }
        return ret;
    }
    
    /* Return a JSON string from the string list. */
    private static void createJSONAndWriteFromList(List<List<String>> dataTable, String fileNameHeader)
    {
        String ret = "";

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
                	JSONArray tableJsonArray  = new JSONArray();
                    // Get current row data.
                    List<String> dataRow = dataTable.get(i);

                    // Create a JSONObject object to store row data.
                    JSONObject rowJsonObject = new JSONObject();

                    for(int j=0;j<columnCount;j++)
                    {
                        String columnName = headerRow.get(j);
                        String fieldName = MAPPING_MAP.get(columnName);
                        
                        String columnValue = dataRow.get(j);
                        rowJsonObject.put(fieldName, columnValue);
                        
                        if(MetaDrivenApplicationConstants.SOURCE_ID_STR.equals(columnName)) {
                        	fileNameSuffix = columnValue;
                        }

                    }

                    //tableJsonObject.put("Row " + i, rowJsonObject);
                    tableJsonArray.add(rowJsonObject);
                    String jsonFileName = fileNameHeader + "_" + fileNameSuffix + ".json";
                    writeStringToFile(tableJsonArray.toString(), jsonFileName);
                }
                // Return string format data of JSONObject object.
                //ret = tableJsonArray.toString();

            }
        }
        //return ret;
    }
    
    /* Write string data to a file.*/
    private static void writeStringToFile(String data, String fileName)
    {
        try
        {
            // Get current executing class working directory.
            String currentWorkingFolder = System.getProperty("user.dir");

            // Get file path separator.
            String filePathSeperator = System.getProperty("file.separator");

            // Get the output file absolute path.
            //String filePath = currentWorkingFolder + filePathSeperator + fileName;
            String filePath = "C:\\Users\\ns250044\\Documents\\Project\\Utility\\outputfile\\"+fileName;
            // Create File, FileWriter and BufferedWriter object.
            File file = new File(filePath);

            FileWriter fw = new FileWriter(file);

            BufferedWriter buffWriter = new BufferedWriter(fw);

            // Write string data to the output file, flush and close the buffered writer object.
            buffWriter.write(data);

            buffWriter.flush();

            buffWriter.close();

            System.out.println(filePath + " has been created.");

        }catch(Exception ex)
        {
            System.err.println(ex.getMessage());
        }
    }

}
