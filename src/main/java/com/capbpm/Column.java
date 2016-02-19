package com.capbpm;

public class Column {

	public String columnName;
	public String columnType;
	public boolean isKey;
	
	public Column(String colName, String colType){
		this.columnName = colName;
		this.columnType = colType;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getColumnType() {
		return columnType;
	}
	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}
	public boolean isKey() {
		return isKey;
	}
	public void setKey(boolean isKey) {
		this.isKey = isKey;
	}
	@Override
	public String toString() {
		return "Column [columnName=" + columnName + ", columnType=" + columnType + ", isKey=" + isKey + "]";
	}
	
	
}
