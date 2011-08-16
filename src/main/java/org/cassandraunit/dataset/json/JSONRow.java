package org.cassandraunit.dataset.json;

import java.util.ArrayList;
import java.util.List;

public class JSONRow {

	private String key;
	private List<JSONSuperColumn> superColumns = new ArrayList<JSONSuperColumn>();
	private List<JSONColumn> columns = new ArrayList<JSONColumn>();

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public List<JSONSuperColumn> getSuperColumns() {
		return superColumns;
	}

	public void setSuperColumns(List<JSONSuperColumn> superColumns) {
		this.superColumns = superColumns;
	}

	public void setColumns(List<JSONColumn> columns) {
		this.columns = columns;
	}

	public List<JSONColumn> getColumns() {
		return columns;
	}

}
