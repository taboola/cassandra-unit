package org.cassandraunit.dataset.commons;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.ddl.ColumnType;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class ParsedColumnFamily {

	private String name;
	private ColumnType type = ColumnType.STANDARD;
	private ParsedDataType keyType = ParsedDataType.BytesType;
	private String comparatorType = "BytesType";
	private ParsedDataType subComparatorType = null;
	private ParsedDataType defaultColumnValueType = ParsedDataType.BytesType;
	private List<ParsedColumnMetadata> columnsMetadata = new ArrayList<ParsedColumnMetadata>();
	private List<ParsedRow> rows = new ArrayList<ParsedRow>();

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

	public void setKeyType(ParsedDataType keyType) {
		this.keyType = keyType;
	}

	public ParsedDataType getKeyType() {
		return keyType;
	}

	public void setComparatorType(String comparatorType) {
		this.comparatorType = comparatorType;
	}

	public String getComparatorType() {
		return comparatorType;
	}

	public void setSubComparatorType(ParsedDataType subComparatorType) {
		this.subComparatorType = subComparatorType;
	}

	public ParsedDataType getSubComparatorType() {
		return subComparatorType;
	}

	public void setDefaultColumnValueType(ParsedDataType defaultColumnValueType) {
		this.defaultColumnValueType = defaultColumnValueType;
	}

	public ParsedDataType getDefaultColumnValueType() {
		return defaultColumnValueType;
	}

	public void setRows(List<ParsedRow> rows) {
		this.rows = rows;
	}

	public List<ParsedRow> getRows() {
		return rows;
	}

	public List<ParsedColumnMetadata> getColumnsMetadata() {
		return columnsMetadata;
	}

	public void setColumnsMetadata(List<ParsedColumnMetadata> columnsMetadata) {
		this.columnsMetadata = columnsMetadata;
	}

}
