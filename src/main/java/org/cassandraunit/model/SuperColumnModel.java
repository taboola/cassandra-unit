package org.cassandraunit.model;

import java.util.ArrayList;
import java.util.List;

import org.cassandraunit.type.GenericType;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class SuperColumnModel {

	private GenericType name;
	private List<ColumnModel> columns = new ArrayList<ColumnModel>();

	public GenericType getName() {
		return name;
	}

	public void setName(GenericType name) {
		this.name = name;
	}

	public List<ColumnModel> getColumns() {
		return columns;
	}

	public void setColumns(List<ColumnModel> columns) {
		this.columns = columns;
	}

}
