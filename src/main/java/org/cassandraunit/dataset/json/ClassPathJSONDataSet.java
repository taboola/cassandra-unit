package org.cassandraunit.dataset.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.ParseException;
import org.cassandraunit.model.ColumnFamilyModel;
import org.cassandraunit.model.KeyspaceModel;
import org.cassandraunit.model.StrategyModel;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class ClassPathJSONDataSet implements DataSet {

	private KeyspaceModel keyspace = null;

	public ClassPathJSONDataSet(String dataSetLocation) {
		JSONKeyspace jsonKeyspace;
		jsonKeyspace = getJSONKeyspace(dataSetLocation);
		mapJSONKeyspaceToModel(jsonKeyspace);

	}

	private JSONKeyspace getJSONKeyspace(String dataSetLocation) {
		InputStream inputDataSetLocation = this.getClass().getResourceAsStream("/" + dataSetLocation);
		if (inputDataSetLocation == null) {
			throw new ParseException("Dataset not found in classpath");
		}

		ObjectMapper jsonMapper = new ObjectMapper();
		try {
			return jsonMapper.readValue(inputDataSetLocation, JSONKeyspace.class);
		} catch (JsonParseException e) {
			throw new ParseException(e);
		} catch (JsonMappingException e) {
			throw new ParseException(e);
		} catch (IOException e) {
			throw new ParseException(e);
		}
	}

	private void mapJSONKeyspaceToModel(JSONKeyspace jsonKeyspace) {
		/* keyspace */
		keyspace = new KeyspaceModel();
		if (jsonKeyspace.getName() == null) {
			throw new ParseException("Keyspace name is mandatory");
		}
		keyspace.setName(jsonKeyspace.getName());

		/* optional conf */
		if (jsonKeyspace.getReplicationFactor() != 0) {
			keyspace.setReplicationFactor(jsonKeyspace.getReplicationFactor());
		}

		if (jsonKeyspace.getStrategy() != null) {
			try {
				keyspace.setStrategy(StrategyModel.fromValue(jsonKeyspace.getStrategy()));
			} catch (IllegalArgumentException e) {
				throw new ParseException("Invalid keyspace Strategy");
			}
		}

		mapsJSONColumnFamiliesToColumnFamiliesModel(jsonKeyspace);

	}

	private void mapsJSONColumnFamiliesToColumnFamiliesModel(JSONKeyspace jsonKeyspace) {
		if (jsonKeyspace.getColumnFamilies() != null) {
			/* there is column families to integrate */
			for (JSONColumnFamily jsonColumnFamily : jsonKeyspace.getColumnFamilies()) {
				keyspace.getColumnFamilies().add(mapJSONColumnFamilyToColumnFamilyModel(jsonColumnFamily));
			}
		}

	}

	private ColumnFamilyModel mapJSONColumnFamilyToColumnFamilyModel(JSONColumnFamily jsonColumnFamily) {

		ColumnFamilyModel columnFamily = new ColumnFamilyModel();

		/* structure information */
		if (jsonColumnFamily.getName() == null) {
			throw new ParseException("Column Family Name is missing");
		}
		columnFamily.setName(jsonColumnFamily.getName());
		if (jsonColumnFamily.getType() != null) {
			columnFamily.setType(ColumnType.valueOf(jsonColumnFamily.getType().toString()));
		}

		if (jsonColumnFamily.getKeyType() != null) {
			columnFamily.setKeyType(ComparatorType.getByClassName(jsonColumnFamily.getKeyType().name()));
		}

		if (jsonColumnFamily.getComparatorType() != null) {
			columnFamily.setComparatorType(ComparatorType.getByClassName(jsonColumnFamily.getComparatorType().name()));
		}

		if (jsonColumnFamily.getSubComparatorType() != null) {
			columnFamily.setSubComparatorType(ComparatorType.getByClassName(jsonColumnFamily.getSubComparatorType()
					.name()));
		}

		if (jsonColumnFamily.getDefaultColumnValueType() != null) {
			columnFamily.setDefaultColumnValueType(ComparatorType.getByClassName(jsonColumnFamily
					.getDefaultColumnValueType().name()));
		}

		return columnFamily;
	}

	@Override
	public KeyspaceModel getKeyspace() {
		return keyspace;
	}

	@Override
	public List<ColumnFamilyModel> getColumnFamilies() {
		return keyspace.getColumnFamilies();
	}

}
