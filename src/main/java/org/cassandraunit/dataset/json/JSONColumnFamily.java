package org.cassandraunit.dataset.json;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.ddl.ColumnType;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class JSONColumnFamily {

	private String name;
	private ColumnType type = ColumnType.STANDARD;
	private JSONDataType keyType = JSONDataType.BytesType;
	private JSONDataType comparatorType = JSONDataType.BytesType;
	private JSONDataType subComparatorType = null;
	private JSONDataType defaultColumnValueType = JSONDataType.BytesType;
	private List<JSONRow> rows = new ArrayList<JSONRow>();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ColumnType getType() {
		return type;
	}

	public void setType(ColumnType type) {
		this.type = type;
	}

	public void setKeyType(JSONDataType keyType) {
		this.keyType = keyType;
	}

	public JSONDataType getKeyType() {
		return keyType;
	}

	public void setComparatorType(JSONDataType comparatorType) {
		this.comparatorType = comparatorType;
	}

	public JSONDataType getComparatorType() {
		return comparatorType;
	}

	public void setSubComparatorType(JSONDataType subComparatorType) {
		this.subComparatorType = subComparatorType;
	}

	public JSONDataType getSubComparatorType() {
		return subComparatorType;
	}

	public void setDefaultColumnValueType(JSONDataType defaultColumnValueType) {
		this.defaultColumnValueType = defaultColumnValueType;
	}

	public JSONDataType getDefaultColumnValueType() {
		return defaultColumnValueType;
	}

	public void setRows(List<JSONRow> rows) {
		this.rows = rows;
	}

	public List<JSONRow> getRows() {
		return rows;
	}

}
