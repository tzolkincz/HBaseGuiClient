package cz.zcu.kiv.hbaseguiclient.model;

import java.util.Map;

public class TableRowDataModel {

	String rowKey;
	Map<String, String> kvs;

	public TableRowDataModel(String rowKey, Map<String, String> row) {
		this.rowKey = rowKey;
		this.kvs = row;
	}

	public String getRowKey() {
		return rowKey;
	}

	public String getValue(String columnName) {
		return kvs.get(columnName);
	}

	public void setValue(String columnName, String newValue) {
		//@TODO fire query to update table row
		System.out.println("Editing with change database value is not implemented yet");
	}

}
