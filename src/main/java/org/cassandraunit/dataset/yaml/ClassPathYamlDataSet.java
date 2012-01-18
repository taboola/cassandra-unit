package org.cassandraunit.dataset.yaml;

import java.io.InputStream;

import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.ParseException;

public class ClassPathYamlDataSet extends AbstractYamlDataSet implements DataSet {

	private String dataSetLocation = null;

	public ClassPathYamlDataSet(String dataSetLocation) {
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
