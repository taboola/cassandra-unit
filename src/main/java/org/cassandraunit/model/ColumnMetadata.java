package org.cassandraunit.model;

import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.apache.cassandra.thrift.IndexType;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class ColumnMetadata {

	private String columnName = null;
	private ComparatorType validationClass = null;
	private ColumnIndexType columnIndexType = null;

	public ColumnMetadata(String columnName, ComparatorType validationClass, ColumnIndexType indexType) {
		super();
		this.columnName = columnName;
		this.validationClass = validationClass;
		this.setColumnIndexType(indexType);
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public ComparatorType getValidationClass() {
		return validationClass;
	}

	public void setValidationClass(ComparatorType validationClass) {
		this.validationClass = validationClass;
	}

	public ColumnIndexType getColumnIndexType() {
		return columnIndexType;
	}

	public void setColumnIndexType(ColumnIndexType columnIndexType) {
		this.columnIndexType = columnIndexType;
	}

}
