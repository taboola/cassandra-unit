package org.cassandraunit.dataset.json;

import java.io.InputStream;

import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.ParseException;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class ClassPathJsonDataSet extends AbstractJsonDataSet implements DataSet {

	private String dataSetLocation;

	public ClassPathJsonDataSet(String dataSetLocation) {
		this.dataSetLocation = dataSetLocation;
		if (getInputDataSetLocation() == null) {
			throw new ParseException("Dataset not found");
		}
	}

	protected InputStream getInputDataSetLocation() {
		InputStream inputDataSetLocation = this.getClass().getResourceAsStream("/" + dataSetLocation);
		return inputDataSetLocation;
	}

}
