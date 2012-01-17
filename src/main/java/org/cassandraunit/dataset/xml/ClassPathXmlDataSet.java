package org.cassandraunit.dataset.xml;

import java.io.InputStream;

import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.ParseException;

public class ClassPathXmlDataSet extends AbstractXmlDataSet implements DataSet {
	private String dataSetLocation = null;

	public ClassPathXmlDataSet(String dataSetLocation) {
		this.dataSetLocation = dataSetLocation;
		if (getInputDataSetLocation() == null) {
			throw new ParseException("Dataset not found");
		}
	}

	@Override
	protected InputStream getInputDataSetLocation() {
		InputStream inputDataSetLocation = this.getClass().getResourceAsStream("/" + dataSetLocation);
		return inputDataSetLocation;
	}

}
