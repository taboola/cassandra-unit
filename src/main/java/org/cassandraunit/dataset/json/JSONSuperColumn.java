package org.cassandraunit.dataset.json;

import java.util.ArrayList;
import java.util.List;

public class JSONSuperColumn {

	private String name;
	private List<JSONColumn> columns = new ArrayList<JSONColumn>();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<JSONColumn> getColumns() {
		return columns;
	}

	public void setColumns(List<JSONColumn> columns) {
		this.columns = columns;
	}

}
