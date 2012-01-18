package org.cassandraunit.dataset.yaml;

import java.io.InputStream;

import org.cassandraunit.dataset.DataSet;

public class ClassPathYamlDataSet extends AbstractYamlDataSet implements DataSet {

	public ClassPathYamlDataSet(String dataSetLocation) {
		super(dataSetLocation);
	}

	@Override
	protected InputStream getInputDataSetLocation(String dataSetLocation) {
		InputStream inputDataSetLocation = this.getClass().getResourceAsStream("/" + dataSetLocation);
		return inputDataSetLocation;
	}

}
