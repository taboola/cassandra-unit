package org.cassandraunit.dataset.xml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.cassandraunit.dataset.DataSet;

public class FileXmlDataSet extends AbstractXmlDataSet implements DataSet {

	public FileXmlDataSet(String dataSetLocation) {
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
