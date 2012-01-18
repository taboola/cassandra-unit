package org.cassandraunit.dataset.yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.cassandraunit.dataset.DataSet;

public class FileYamlDataSet extends AbstractYamlDataSet implements DataSet {

	String dataSetLocation = null;

	public FileYamlDataSet(String dataSetLocation) {
		super(dataSetLocation);
	}

	@Override
	protected InputStream getInputDataSetLocation(String dataSetLocation) {
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
