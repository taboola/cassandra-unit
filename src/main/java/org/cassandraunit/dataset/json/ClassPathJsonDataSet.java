package org.cassandraunit.dataset.json;

import java.io.IOException;
import java.io.InputStream;

import org.cassandraunit.dataset.ParseException;
import org.cassandraunit.dataset.commons.AbstractCommonsParserDataSet;
import org.cassandraunit.dataset.commons.ParsedKeyspace;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class ClassPathJsonDataSet extends AbstractCommonsParserDataSet {

	private String dataSetLocation;

	public ClassPathJsonDataSet(String dataSetLocation) {
		InputStream inputDataSetLocation = this.getClass().getResourceAsStream("/" + dataSetLocation);
		if (inputDataSetLocation == null) {
			throw new ParseException("Dataset not found in classpath");
		}
		this.dataSetLocation = dataSetLocation;
	}

	protected ParsedKeyspace getParsedKeyspace() {
		InputStream inputDataSetLocation = this.getClass().getResourceAsStream("/" + dataSetLocation);
		if (inputDataSetLocation == null) {
			throw new ParseException("Dataset not found in classpath");
		}

		ObjectMapper jsonMapper = new ObjectMapper();
		try {
			return jsonMapper.readValue(inputDataSetLocation, ParsedKeyspace.class);
		} catch (JsonParseException e) {
			throw new ParseException(e);
		} catch (JsonMappingException e) {
			throw new ParseException(e);
		} catch (IOException e) {
			throw new ParseException(e);
		}
	}

}
