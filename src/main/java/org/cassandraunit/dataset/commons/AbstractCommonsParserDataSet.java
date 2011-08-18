package org.cassandraunit.dataset.commons;

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

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public abstract class AbstractCommonsParserDataSet implements DataSet {

	protected KeyspaceModel keyspace = null;

	protected abstract ParsedKeyspace getParsedKeyspace();

	@Override
	public KeyspaceModel getKeyspace() {
		if (keyspace == null) {
			mapParsedKeyspaceToModel(getParsedKeyspace());
		}
		return keyspace;
	}

	@Override
	public List<ColumnFamilyModel> getColumnFamilies() {
		if (keyspace == null) {
			mapParsedKeyspaceToModel(getParsedKeyspace());
		}
		return keyspace.getColumnFamilies();
	}

	protected void mapParsedKeyspaceToModel(ParsedKeyspace parsedKeyspace) {
		if (parsedKeyspace == null) {
			throw new ParseException("dataSet is empty");
		}
		/* keyspace */
		keyspace = new KeyspaceModel();
		if (parsedKeyspace.getName() == null) {
			throw new ParseException("Keyspace name is mandatory");
		}
		keyspace.setName(parsedKeyspace.getName());

		/* optional conf */
		if (parsedKeyspace.getReplicationFactor() != 0) {
			keyspace.setReplicationFactor(parsedKeyspace.getReplicationFactor());
		}

		if (parsedKeyspace.getStrategy() != null) {
			try {
				keyspace.setStrategy(StrategyModel.fromValue(parsedKeyspace.getStrategy()));
			} catch (IllegalArgumentException e) {
				throw new ParseException("Invalid keyspace Strategy");
			}
		}

		mapsParsedColumnFamiliesToColumnFamiliesModel(parsedKeyspace);

	}

	private void mapsParsedColumnFamiliesToColumnFamiliesModel(ParsedKeyspace parsedKeyspace) {
		if (parsedKeyspace.getColumnFamilies() != null) {
			/* there is column families to integrate */
			for (ParsedColumnFamily parsedColumnFamily : parsedKeyspace.getColumnFamilies()) {
				keyspace.getColumnFamilies().add(mapParsedColumnFamilyToColumnFamilyModel(parsedColumnFamily));
			}
		}

	}

	private ColumnFamilyModel mapParsedColumnFamilyToColumnFamilyModel(ParsedColumnFamily parsedColumnFamily) {

		ColumnFamilyModel columnFamily = new ColumnFamilyModel();

		/* structure information */
		if (parsedColumnFamily == null || parsedColumnFamily.getName() == null) {
			throw new ParseException("Column Family Name is missing");
		}
		columnFamily.setName(parsedColumnFamily.getName());
		if (parsedColumnFamily.getType() != null) {
			columnFamily.setType(ColumnType.valueOf(parsedColumnFamily.getType().toString()));
		}

		if (parsedColumnFamily.getKeyType() != null) {
			columnFamily.setKeyType(ComparatorType.getByClassName(parsedColumnFamily.getKeyType().name()));
		}

		if (parsedColumnFamily.getComparatorType() != null) {
			columnFamily
					.setComparatorType(ComparatorType.getByClassName(parsedColumnFamily.getComparatorType().name()));
		}

		if (parsedColumnFamily.getSubComparatorType() != null) {
			columnFamily.setSubComparatorType(ComparatorType.getByClassName(parsedColumnFamily.getSubComparatorType()
					.name()));
		}

		if (parsedColumnFamily.getDefaultColumnValueType() != null) {
			columnFamily.setDefaultColumnValueType(ComparatorType.getByClassName(parsedColumnFamily
					.getDefaultColumnValueType().name()));
		}

		/* data information */
		columnFamily.setRows(mapParsedRowsToRowsModel(parsedColumnFamily, columnFamily.getKeyType(),
				columnFamily.getComparatorType(), columnFamily.getSubComparatorType(),
				columnFamily.getDefaultColumnValueType()));

		return columnFamily;
	}

	private List<RowModel> mapParsedRowsToRowsModel(ParsedColumnFamily parsedColumnFamily, ComparatorType keyType,
			ComparatorType comparatorType, ComparatorType subComparatorType, ComparatorType defaultColumnValueType) {
		List<RowModel> rowsModel = new ArrayList<RowModel>();
		for (ParsedRow jsonRow : parsedColumnFamily.getRows()) {
			rowsModel.add(mapsParsedRowToRowModel(jsonRow, keyType, comparatorType, subComparatorType,
					defaultColumnValueType));
		}
		return rowsModel;
	}

	private RowModel mapsParsedRowToRowModel(ParsedRow parsedRow, ComparatorType keyType,
			ComparatorType comparatorType, ComparatorType subComparatorType, ComparatorType defaultColumnValueType) {
		RowModel row = new RowModel();

		row.setKey(new GenericType(parsedRow.getKey(), GenericTypeEnum.fromValue(keyType.getTypeName())));
		row.setColumns(mapParsedColumnsToColumnsModel(parsedRow.getColumns(), comparatorType, defaultColumnValueType));
		row.setSuperColumns(mapParsedSuperColumnsToSuperColumnsModel(parsedRow.getSuperColumns(), comparatorType,
				subComparatorType, defaultColumnValueType));
		return row;
	}

	private List<SuperColumnModel> mapParsedSuperColumnsToSuperColumnsModel(List<ParsedSuperColumn> parsedSuperColumns,
			ComparatorType comparatorType, ComparatorType subComparatorType, ComparatorType defaultColumnValueType) {
		List<SuperColumnModel> columnsModel = new ArrayList<SuperColumnModel>();
		for (ParsedSuperColumn parsedSuperColumn : parsedSuperColumns) {
			columnsModel.add(mapParsedSuperColumnToSuperColumnModel(parsedSuperColumn, comparatorType,
					subComparatorType, defaultColumnValueType));
		}

		return columnsModel;
	}

	private SuperColumnModel mapParsedSuperColumnToSuperColumnModel(ParsedSuperColumn parsedSuperColumn,
			ComparatorType comparatorType, ComparatorType subComparatorType, ComparatorType defaultColumnValueType) {
		SuperColumnModel superColumnModel = new SuperColumnModel();

		superColumnModel.setName(new GenericType(parsedSuperColumn.getName(), GenericTypeEnum.fromValue(comparatorType
				.getTypeName())));

		superColumnModel.setColumns(mapParsedColumnsToColumnsModel(parsedSuperColumn.getColumns(), subComparatorType,
				defaultColumnValueType));
		return superColumnModel;
	}

	private List<ColumnModel> mapParsedColumnsToColumnsModel(List<ParsedColumn> parsedColumns,
			ComparatorType comparatorType, ComparatorType defaultColumnValueType) {
		List<ColumnModel> columnsModel = new ArrayList<ColumnModel>();
		for (ParsedColumn jsonColumn : parsedColumns) {
			columnsModel.add(mapParsedColumnToColumnModel(jsonColumn, comparatorType, defaultColumnValueType));
		}
		return columnsModel;
	}

	private ColumnModel mapParsedColumnToColumnModel(ParsedColumn parsedColumn, ComparatorType comparatorType,
			ComparatorType defaultColumnValueType) {
		ColumnModel columnModel = new ColumnModel();

		if (comparatorType == null) {
			columnModel.setName(new GenericType(parsedColumn.getName(), GenericTypeEnum.BYTES_TYPE));
		} else {
			columnModel.setName(new GenericType(parsedColumn.getName(), GenericTypeEnum.fromValue(comparatorType
					.getTypeName())));
		}

		GenericType columnValue = TypeExtractor.extract(parsedColumn.getValue(), defaultColumnValueType);
		columnModel.setValue(columnValue);
		return columnModel;
	}

}
