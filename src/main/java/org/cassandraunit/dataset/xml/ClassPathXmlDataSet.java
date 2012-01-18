package org.cassandraunit.dataset.xml;

import java.io.InputStream;

import org.cassandraunit.dataset.DataSet;

public class ClassPathXmlDataSet extends AbstractXmlDataSet implements DataSet {

	public ClassPathXmlDataSet(String dataSetLocation) {
		super(dataSetLocation);
	}

	@Override
	protected InputStream getInputDataSetLocation(String dataSetLocation) {
		InputStream inputDataSetLocation = this.getClass().getResourceAsStream("/" + dataSetLocation);
		return inputDataSetLocation;
	}

}
