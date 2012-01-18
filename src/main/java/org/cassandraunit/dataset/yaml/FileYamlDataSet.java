package org.cassandraunit.dataset.yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.ParseException;

public class FileYamlDataSet extends AbstractYamlDataSet implements DataSet {

	String dataSetLocation = null;

	public FileYamlDataSet(String dataSetLocation) {
		super();
		this.dataSetLocation = dataSetLocation;
		if (getInputDataSetLocation() == null) {
			throw new ParseException("Dataset not found");
		}
	}

	@Override
	protected InputStream getInputDataSetLocation() {
		if (dataSetLocation == null) {
			return null;
		}
		try {
			return new FileInputStream(dataSetLocation);
		} catch (FileNotFoundException e) {
			return null;
		}
	}

}
