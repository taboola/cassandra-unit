package org.cassandraunit.dataset.yaml;

import java.io.InputStream;

import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.ParseException;
import org.cassandraunit.dataset.commons.AbstractCommonsParserDataSet;
import org.cassandraunit.dataset.commons.ParsedKeyspace;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

public class ClassPathYamlDataSet extends AbstractCommonsParserDataSet implements DataSet {

	private String dataSetLocation = null;

	public ClassPathYamlDataSet(String dataSetLocation) {
		InputStream inputDataSetLocation = this.getClass().getResourceAsStream("/" + dataSetLocation);
		if (inputDataSetLocation == null) {
			throw new ParseException("Dataset not found in classpath");
		}
		this.dataSetLocation = dataSetLocation;
	}

	@Override
	protected ParsedKeyspace getParsedKeyspace() {
		InputStream inputDataSetLocation = this.getClass().getResourceAsStream("/" + dataSetLocation);
		if (inputDataSetLocation == null) {
			throw new ParseException("Dataset not found in classpath");
		}

		Yaml yaml = new Yaml();
		try {
			ParsedKeyspace keyspace = yaml.loadAs(inputDataSetLocation, ParsedKeyspace.class);
			return keyspace;
		} catch (YAMLException e) {
			throw new ParseException(e);
		}
	}

}
