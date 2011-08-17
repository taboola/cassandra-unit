package org.cassandraunit.dataset.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.ParseException;
import org.cassandraunit.model.ColumnFamilyModel;
import org.cassandraunit.model.ColumnModel;
import org.cassandraunit.model.KeyspaceModel;
import org.cassandraunit.model.RowModel;
import org.cassandraunit.model.StrategyModel;
import org.cassandraunit.model.SuperColumnModel;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;
import org.cassandraunit.utils.TypeExtractor;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
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

		/* data information */
		columnFamily.setRows(mapJSONRowsToRowsModel(jsonColumnFamily, columnFamily.getKeyType(),
				columnFamily.getComparatorType(), columnFamily.getSubComparatorType(),
				columnFamily.getDefaultColumnValueType()));

		return columnFamily;
	}

	private List<RowModel> mapJSONRowsToRowsModel(JSONColumnFamily jsonColumnFamily, ComparatorType keyType,
			ComparatorType comparatorType, ComparatorType subComparatorType, ComparatorType defaultColumnValueType) {
		List<RowModel> rowsModel = new ArrayList<RowModel>();
		for (JSONRow jsonRow : jsonColumnFamily.getRows()) {
			rowsModel.add(mapsJSONRowToRowModel(jsonRow, keyType, comparatorType, subComparatorType,
					defaultColumnValueType));
		}
		return rowsModel;
	}

	private RowModel mapsJSONRowToRowModel(JSONRow jsonRow, ComparatorType keyType, ComparatorType comparatorType,
			ComparatorType subComparatorType, ComparatorType defaultColumnValueType) {
		RowModel row = new RowModel();

		row.setKey(new GenericType(jsonRow.getKey(), GenericTypeEnum.fromValue(keyType.getTypeName())));
		row.setColumns(mapXmlColumnsToColumnsModel(jsonRow.getColumns(), comparatorType, defaultColumnValueType));
		row.setSuperColumns(mapXmlSuperColumnsToSuperColumnsModel(jsonRow.getSuperColumns(), comparatorType,
				subComparatorType, defaultColumnValueType));
		return row;
	}

	private List<SuperColumnModel> mapXmlSuperColumnsToSuperColumnsModel(List<JSONSuperColumn> jsonSuperColumns,
			ComparatorType comparatorType, ComparatorType subComparatorType, ComparatorType defaultColumnValueType) {
		List<SuperColumnModel> columnsModel = new ArrayList<SuperColumnModel>();
		for (JSONSuperColumn jsonSuperColumn : jsonSuperColumns) {
			columnsModel.add(mapXmlSuperColumnToSuperColumnModel(jsonSuperColumn, comparatorType, subComparatorType,
					defaultColumnValueType));
		}

		return columnsModel;
	}

	private SuperColumnModel mapXmlSuperColumnToSuperColumnModel(JSONSuperColumn jsonSuperColumn,
			ComparatorType comparatorType, ComparatorType subComparatorType, ComparatorType defaultColumnValueType) {
		SuperColumnModel superColumnModel = new SuperColumnModel();

		superColumnModel.setName(new GenericType(jsonSuperColumn.getName(), GenericTypeEnum.fromValue(comparatorType
				.getTypeName())));

		superColumnModel.setColumns(mapXmlColumnsToColumnsModel(jsonSuperColumn.getColumns(), subComparatorType,
				defaultColumnValueType));
		return superColumnModel;
	}

	private List<ColumnModel> mapXmlColumnsToColumnsModel(List<JSONColumn> jsonColumns, ComparatorType comparatorType,
			ComparatorType defaultColumnValueType) {
		List<ColumnModel> columnsModel = new ArrayList<ColumnModel>();
		for (JSONColumn jsonColumn : jsonColumns) {
			columnsModel.add(mapXmlColumnToColumnModel(jsonColumn, comparatorType, defaultColumnValueType));
		}
		return columnsModel;
	}

	private ColumnModel mapXmlColumnToColumnModel(JSONColumn jsonColumn, ComparatorType comparatorType,
			ComparatorType defaultColumnValueType) {
		ColumnModel columnModel = new ColumnModel();

		if (comparatorType == null) {
			columnModel.setName(new GenericType(jsonColumn.getName(), GenericTypeEnum.BYTES_TYPE));
		} else {
			columnModel.setName(new GenericType(jsonColumn.getName(), GenericTypeEnum.fromValue(comparatorType
					.getTypeName())));
		}

		GenericType columnValue = TypeExtractor.extract(jsonColumn.getValue(), defaultColumnValueType);
		columnModel.setValue(columnValue);
		return columnModel;
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
