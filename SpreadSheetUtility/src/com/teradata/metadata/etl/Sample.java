package com.teradata.metadata.etl;

import net.sf.json.JSONObject;

public class Sample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		JSONObject rowJsonObject = new JSONObject();
		rowJsonObject.put("src1", "hi");
		rowJsonObject.put("src2", "why");
		rowJsonObject.put("src1", "hello");
		System.out.println(rowJsonObject.toString());
	}

}
