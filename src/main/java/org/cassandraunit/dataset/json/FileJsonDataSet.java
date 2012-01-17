package org.cassandraunit.dataset.json;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.ParseException;

public class FileJsonDataSet extends AbstractJsonDataSet implements DataSet {

	String dataSetLocation = null;

	public FileJsonDataSet(String dataSetLocation) {
		this.dataSetLocation = dataSetLocation;
		if (getInputDataSetLocation() == null) {
			throw new ParseException("Dataset not found");
		}
	}

	@Override
	protected InputStream getInputDataSetLocation() {
		try {
			return new FileInputStream(dataSetLocation);
		} catch (FileNotFoundException e) {
			return null;
		}
	}

}
