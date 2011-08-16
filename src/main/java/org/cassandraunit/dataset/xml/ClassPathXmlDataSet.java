package org.cassandraunit.dataset.xml;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.ParseException;
import org.cassandraunit.model.ColumnFamilyModel;
import org.cassandraunit.model.ColumnModel;
import org.cassandraunit.model.KeyspaceModel;
import org.cassandraunit.model.RowModel;
import org.cassandraunit.model.SuperColumnModel;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;
import org.cassandraunit.utils.TypeExtractor;
import org.xml.sax.SAXException;

public class ClassPathXmlDataSet implements DataSet {

	private KeyspaceModel keyspace = null;
	private List<ColumnFamilyModel> columnFamilies = null;

	public ClassPathXmlDataSet(String dataSetLocation) {
		org.cassandraunit.dataset.xml.Keyspace xmlKeyspace;
		xmlKeyspace = getXmlKeyspace(dataSetLocation);
		mapXmlKeyspaceToModel(xmlKeyspace);
	}

	private org.cassandraunit.dataset.xml.Keyspace getXmlKeyspace(String dataSetLocation) {
		InputStream inputDataSetLocation = this.getClass().getResourceAsStream("/" + dataSetLocation);
		if (inputDataSetLocation == null) {
			throw new ParseException("Dataset not found in classpath");
		}

		try {
			Unmarshaller unmarshaller = getUnmarshaller();
			org.cassandraunit.dataset.xml.Keyspace xmlKeyspace = (org.cassandraunit.dataset.xml.Keyspace) unmarshaller
					.unmarshal(inputDataSetLocation);
			return xmlKeyspace;
		} catch (JAXBException e) {
			throw new ParseException(e);
		} catch (SAXException e) {
			throw new ParseException(e);
		} catch (URISyntaxException e) {
			throw new ParseException(e);
		}

	}

	private Unmarshaller getUnmarshaller() throws JAXBException, SAXException, URISyntaxException {
		JAXBContext jc = JAXBContext.newInstance(org.cassandraunit.dataset.xml.Keyspace.class);
		Unmarshaller unmarshaller = jc.createUnmarshaller();

		SchemaFactory sf = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);

		Schema schema = sf.newSchema(this.getClass().getResource("/dataset.xsd"));

		unmarshaller.setSchema(schema);

		return unmarshaller;
	}

	private void mapXmlKeyspaceToModel(org.cassandraunit.dataset.xml.Keyspace xmlKeyspace) {

		/* keyspace */
		keyspace = new KeyspaceModel();
		keyspace.setName(xmlKeyspace.getName());

		/* optional conf */
		if (xmlKeyspace.getReplicationFactor() != null) {
			keyspace.setReplicationFactor(xmlKeyspace.getReplicationFactor());
		}

		if (xmlKeyspace.getStrategy() != null) {
			keyspace.setStategy(xmlKeyspace.getStrategy());
		}

		mapsXmlColumnFamiliesToColumnFamiliesModel(xmlKeyspace);
	}

	private void mapsXmlColumnFamiliesToColumnFamiliesModel(org.cassandraunit.dataset.xml.Keyspace xmlKeyspace) {
		columnFamilies = new ArrayList<ColumnFamilyModel>();
		if (xmlKeyspace.getColumnFamilies() != null) {
			/* there is column families to integrate */
			for (org.cassandraunit.dataset.xml.ColumnFamily xmlColumnFamily : xmlKeyspace.getColumnFamilies()
					.getColumnFamily()) {
				columnFamilies.add(mapXmlColumnFamilyToColumnFamilyModel(xmlColumnFamily));
			}
		}
	}

	private ColumnFamilyModel mapXmlColumnFamilyToColumnFamilyModel(
			org.cassandraunit.dataset.xml.ColumnFamily xmlColumnFamily) {

		ColumnFamilyModel columnFamily = new ColumnFamilyModel();

		/* structure information */
		columnFamily.setName(xmlColumnFamily.getName());
		if (xmlColumnFamily.getType() != null) {
			columnFamily.setType(ColumnType.valueOf(xmlColumnFamily.getType().toString()));
		}

		if (xmlColumnFamily.getKeyType() != null) {
			columnFamily.setKeyType(ComparatorType.getByClassName(xmlColumnFamily.getKeyType().value()));
		}

		if (xmlColumnFamily.getComparatorType() != null) {
			columnFamily.setComparatorType(ComparatorType.getByClassName(xmlColumnFamily.getComparatorType().value()));
		}

		if (xmlColumnFamily.getSubComparatorType() != null) {
			columnFamily.setSubComparatorType(ComparatorType.getByClassName(xmlColumnFamily.getSubComparatorType()
					.value()));
		}

		if (xmlColumnFamily.getDefaultColumnValueType() != null) {
			columnFamily.setDefaultColumnValueType(ComparatorType.getByClassName(xmlColumnFamily
					.getDefaultColumnValueType().value()));
		}

		/* data information */
		columnFamily.setRows(mapXmlRowsToRowsModel(xmlColumnFamily, columnFamily.getKeyType(),
				columnFamily.getComparatorType(), columnFamily.getSubComparatorType(),
				columnFamily.getDefaultColumnValueType()));

		return columnFamily;
	}

	private List<RowModel> mapXmlRowsToRowsModel(org.cassandraunit.dataset.xml.ColumnFamily xmlColumnFamily,
			ComparatorType keyType, ComparatorType comparatorType, ComparatorType subcomparatorType,
			ComparatorType defaultColumnValueType) {
		List<RowModel> rowsModel = new ArrayList<RowModel>();
		for (RowType rowType : xmlColumnFamily.getRow()) {
			rowsModel.add(mapsXmlRowToRowModel(rowType, keyType, comparatorType, subcomparatorType,
					defaultColumnValueType));
		}
		return rowsModel;
	}

	private RowModel mapsXmlRowToRowModel(RowType rowType, ComparatorType keyType, ComparatorType comparatorType,
			ComparatorType subComparatorType, ComparatorType defaultColumnValueType) {
		RowModel row = new RowModel();

		row.setKey(new GenericType(rowType.getKey(), GenericTypeEnum.fromValue(keyType.getTypeName())));
		row.setColumns(mapXmlColumnsToColumnsModel(rowType.getColumn(), comparatorType, defaultColumnValueType));
		row.setSuperColumns(mapXmlSuperColumnsToSuperColumnsModel(rowType.getSuperColumn(), comparatorType,
				subComparatorType, defaultColumnValueType));
		return row;
	}

	/**
	 * map an xml super columns to a super columns
	 * 
	 * @param superColumns
	 *            xml super columns
	 * @param subComparatorType
	 * @param comparatorType
	 * @return super columns
	 */
	private List<SuperColumnModel> mapXmlSuperColumnsToSuperColumnsModel(
			List<org.cassandraunit.dataset.xml.SuperColumnType> superColumns, ComparatorType comparatorType,
			ComparatorType subComparatorType, ComparatorType defaultColumnValueType) {
		List<SuperColumnModel> columnsModel = new ArrayList<SuperColumnModel>();
		for (SuperColumnType superColumnType : superColumns) {
			columnsModel.add(mapXmlSuperColumnToSuperColumnModel(superColumnType, comparatorType, subComparatorType,
					defaultColumnValueType));
		}

		return columnsModel;
	}

	/**
	 * map an xml super colmun to a super column
	 * 
	 * @param superColumn
	 *            xml super column
	 * @param subComparatorType
	 * @param comparatorType
	 * @return supercolumn
	 */
	private SuperColumnModel mapXmlSuperColumnToSuperColumnModel(SuperColumnType superColumn,
			ComparatorType comparatorType, ComparatorType subComparatorType, ComparatorType defaultColumnValueType) {
		SuperColumnModel superColumnModel = new SuperColumnModel();

		superColumnModel.setName(new GenericType(superColumn.getName(), GenericTypeEnum.fromValue(comparatorType
				.getTypeName())));

		superColumnModel.setColumns(mapXmlColumnsToColumnsModel(superColumn.getColumn(), subComparatorType,
				defaultColumnValueType));
		return superColumnModel;
	}

	/**
	 * map an xml column to a column
	 * 
	 * @param column
	 *            xml column
	 * @return column
	 */
	private ColumnModel mapXmlColumnToColumnModel(org.cassandraunit.dataset.xml.ColumnType column,
			ComparatorType comparatorType, ComparatorType defaultColumnValueType) {
		ColumnModel columnModel = new ColumnModel();

		if (comparatorType == null) {
			columnModel.setName(new GenericType(column.getName(), GenericTypeEnum.BYTES_TYPE));
		} else {
			columnModel.setName(new GenericType(column.getName(), GenericTypeEnum.fromValue(comparatorType
					.getTypeName())));
		}

		GenericType columnValue = TypeExtractor.extract(column.getValue(), defaultColumnValueType);
		columnModel.setValue(columnValue);

		return columnModel;
	}

	/**
	 * map an xml columns to columns
	 * 
	 * @param columns
	 *            xml column
	 * @return columns
	 */
	private List<ColumnModel> mapXmlColumnsToColumnsModel(List<org.cassandraunit.dataset.xml.ColumnType> columns,
			ComparatorType columnNameComparatorType, ComparatorType defaultColumnValueType) {
		List<ColumnModel> columnsModel = new ArrayList<ColumnModel>();
		for (org.cassandraunit.dataset.xml.ColumnType columnType : columns) {
			columnsModel.add(mapXmlColumnToColumnModel(columnType, columnNameComparatorType, defaultColumnValueType));
		}
		return columnsModel;
	}

	@Override
	public KeyspaceModel getKeyspace() {
		return keyspace;
	}

	@Override
	public List<ColumnFamilyModel> getColumnFamilies() {
		return columnFamilies;
	}

}
