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

import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.apache.commons.lang.StringUtils;
import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.ParseException;
import org.cassandraunit.model.ColumnFamilyModel;
import org.cassandraunit.model.ColumnMetadata;
import org.cassandraunit.model.ColumnModel;
import org.cassandraunit.model.KeyspaceModel;
import org.cassandraunit.model.RowModel;
import org.cassandraunit.model.StrategyModel;
import org.cassandraunit.model.SuperColumnModel;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;
import org.cassandraunit.utils.ComparatorTypeHelper;
import org.cassandraunit.utils.TypeExtractor;
import org.xml.sax.SAXException;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public abstract class AbstractXmlDataSet implements DataSet {

	private String dataSetLocation = null;

	private KeyspaceModel keyspace = null;

	public AbstractXmlDataSet(String dataSetLocation) {
		this.dataSetLocation = dataSetLocation;
		if (getInputDataSetLocation(dataSetLocation) == null) {
			throw new ParseException("Dataset not found");
		}
	}

	private org.cassandraunit.dataset.xml.Keyspace getXmlKeyspace() {
		InputStream inputDataSetLocation = getInputDataSetLocation(dataSetLocation);
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

	protected abstract InputStream getInputDataSetLocation(String dataSetLocation);

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
			keyspace.setStrategy(StrategyModel.fromValue(xmlKeyspace.getStrategy().value()));
		}

		mapsXmlColumnFamiliesToColumnFamiliesModel(xmlKeyspace);
	}

	private void mapsXmlColumnFamiliesToColumnFamiliesModel(org.cassandraunit.dataset.xml.Keyspace xmlKeyspace) {

		if (xmlKeyspace.getColumnFamilies() != null) {
			/* there is column families to integrate */
			for (org.cassandraunit.dataset.xml.ColumnFamily xmlColumnFamily : xmlKeyspace.getColumnFamilies()
					.getColumnFamily()) {
				keyspace.getColumnFamilies().add(mapXmlColumnFamilyToColumnFamilyModel(xmlColumnFamily));
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

		GenericTypeEnum[] typesBelongingCompositeTypeForKeyType = null;
		if (xmlColumnFamily.getKeyType() != null) {
			ComparatorType keyType = ComparatorTypeHelper.verifyAndExtract(xmlColumnFamily.getKeyType());
			columnFamily.setKeyType(keyType);
			if (ComparatorType.COMPOSITETYPE.getTypeName().equals(keyType.getTypeName())) {
				String keyTypeAlias = StringUtils.removeStart(xmlColumnFamily.getKeyType(),
						ComparatorType.COMPOSITETYPE.getTypeName());
				columnFamily.setKeyTypeAlias(keyTypeAlias);
				typesBelongingCompositeTypeForKeyType = ComparatorTypeHelper
						.extractGenericTypesFromTypeAlias(keyTypeAlias);
			}
		}

		GenericTypeEnum[] typesBelongingCompositeTypeForComparatorType = null;
		if (xmlColumnFamily.getComparatorType() != null) {
			ComparatorType comparatorType = ComparatorTypeHelper.verifyAndExtract(xmlColumnFamily.getComparatorType());
			columnFamily.setComparatorType(comparatorType);
			if (ComparatorType.COMPOSITETYPE.getTypeName().equals(comparatorType.getTypeName())) {
				String comparatorTypeAlias = StringUtils.removeStart(xmlColumnFamily.getComparatorType(),
						ComparatorType.COMPOSITETYPE.getTypeName());
				columnFamily.setComparatorTypeAlias(comparatorTypeAlias);
				typesBelongingCompositeTypeForComparatorType = ComparatorTypeHelper
						.extractGenericTypesFromTypeAlias(comparatorTypeAlias);
			}
		}

		if (xmlColumnFamily.getSubComparatorType() != null) {
			columnFamily.setSubComparatorType(ComparatorType.getByClassName(xmlColumnFamily.getSubComparatorType()
					.value()));
		}

		if (xmlColumnFamily.getDefaultColumnValueType() != null) {
			columnFamily.setDefaultColumnValueType(ComparatorType.getByClassName(xmlColumnFamily
					.getDefaultColumnValueType().value()));
		}

		columnFamily.setColumnsMetadata(mapXmlColumsMetadataToColumnsMetadata(xmlColumnFamily.getColumnMetadata()));

		/* data information */
		columnFamily.setRows(mapXmlRowsToRowsModel(xmlColumnFamily, columnFamily.getKeyType(),
				typesBelongingCompositeTypeForKeyType, columnFamily.getComparatorType(),
				typesBelongingCompositeTypeForComparatorType, columnFamily.getSubComparatorType(),
				columnFamily.getDefaultColumnValueType()));

		return columnFamily;
	}

	private List<ColumnMetadata> mapXmlColumsMetadataToColumnsMetadata(
			List<org.cassandraunit.dataset.xml.ColumnMetadata> xmlColumnsMetadata) {

		ArrayList<ColumnMetadata> columnsMetadata = new ArrayList<ColumnMetadata>();

		for (org.cassandraunit.dataset.xml.ColumnMetadata xmlColumnMetadata : xmlColumnsMetadata) {
			columnsMetadata.add(mapXmlColumnMetadataToColumMetadata(xmlColumnMetadata));
		}

		return columnsMetadata;
	}

	private ColumnMetadata mapXmlColumnMetadataToColumMetadata(
			org.cassandraunit.dataset.xml.ColumnMetadata xmlColumnMetadata) {
		ColumnMetadata columnMetadata = new ColumnMetadata();
		columnMetadata.setColumnName(xmlColumnMetadata.getName());
		columnMetadata
				.setValidationClass(ComparatorType.getByClassName(xmlColumnMetadata.getValidationClass().value()));
		if (xmlColumnMetadata.getIndexType() != null) {
			columnMetadata.setColumnIndexType(ColumnIndexType.valueOf(xmlColumnMetadata.getIndexType().value()));
		}

		return columnMetadata;
	}

	private List<RowModel> mapXmlRowsToRowsModel(org.cassandraunit.dataset.xml.ColumnFamily xmlColumnFamily,
			ComparatorType keyType, GenericTypeEnum[] typesBelongingCompositeTypeForKeyType,
			ComparatorType comparatorType, GenericTypeEnum[] typesBelongingCompositeTypeForComparatorType,
			ComparatorType subcomparatorType, ComparatorType defaultColumnValueType) {
		List<RowModel> rowsModel = new ArrayList<RowModel>();
		for (Row rowType : xmlColumnFamily.getRow()) {
			rowsModel.add(mapsXmlRowToRowModel(rowType, keyType, typesBelongingCompositeTypeForKeyType, comparatorType,
					typesBelongingCompositeTypeForComparatorType, subcomparatorType, defaultColumnValueType));
		}
		return rowsModel;
	}

	private RowModel mapsXmlRowToRowModel(Row xmlRow, ComparatorType keyType,
			GenericTypeEnum[] typesBelongingCompositeTypeForKeyType, ComparatorType comparatorType,
			GenericTypeEnum[] typesBelongingCompositeTypeForComparatorType, ComparatorType subComparatorType,
			ComparatorType defaultColumnValueType) {
		RowModel row = new RowModel();

		row.setKey(TypeExtractor.constructGenericType(xmlRow.getKey(), keyType, typesBelongingCompositeTypeForKeyType));

		row.setColumns(mapXmlColumnsToColumnsModel(xmlRow.getColumn(), comparatorType,
				typesBelongingCompositeTypeForComparatorType, defaultColumnValueType));
		row.setSuperColumns(mapXmlSuperColumnsToSuperColumnsModel(xmlRow.getSuperColumn(), comparatorType,
				subComparatorType, defaultColumnValueType));
		return row;
	}

	/**
	 * map an xml super columns to a super columns
	 * 
	 * @param xmlSuperColumns
	 *            xml super columns
	 * @param subComparatorType
	 * @param comparatorType
	 * @return super columns
	 */
	private List<SuperColumnModel> mapXmlSuperColumnsToSuperColumnsModel(List<SuperColumn> xmlSuperColumns,
			ComparatorType comparatorType, ComparatorType subComparatorType, ComparatorType defaultColumnValueType) {
		List<SuperColumnModel> columnsModel = new ArrayList<SuperColumnModel>();
		for (SuperColumn xmlSuperColumnType : xmlSuperColumns) {
			columnsModel.add(mapXmlSuperColumnToSuperColumnModel(xmlSuperColumnType, comparatorType, subComparatorType,
					defaultColumnValueType));
		}

		return columnsModel;
	}

	/**
	 * map an xml super colmun to a super column
	 * 
	 * @param xmlSuperColumn
	 *            xml super column
	 * @param subComparatorType
	 * @param comparatorType
	 * @return supercolumn
	 */
	private SuperColumnModel mapXmlSuperColumnToSuperColumnModel(SuperColumn xmlSuperColumn,
			ComparatorType comparatorType, ComparatorType subComparatorType, ComparatorType defaultColumnValueType) {
		SuperColumnModel superColumnModel = new SuperColumnModel();

		superColumnModel.setName(new GenericType(xmlSuperColumn.getName(), GenericTypeEnum.fromValue(comparatorType
				.getTypeName())));

		superColumnModel.setColumns(mapXmlColumnsToColumnsModel(xmlSuperColumn.getColumn(), subComparatorType, null,
				defaultColumnValueType));
		return superColumnModel;
	}

	/**
	 * map an xml column to a column
	 * 
	 * @param xmlColumn
	 *            xml column
	 * @param typesBelongingCompositeTypeForComparatorType
	 * @return column
	 */
	private ColumnModel mapXmlColumnToColumnModel(Column xmlColumn, ComparatorType comparatorType,
			GenericTypeEnum[] typesBelongingCompositeTypeForComparatorType, ComparatorType defaultColumnValueType) {
		ColumnModel columnModel = new ColumnModel();

		if (comparatorType == null) {
			columnModel.setName(new GenericType(xmlColumn.getName(), GenericTypeEnum.BYTES_TYPE));
		} else if (ComparatorType.COMPOSITETYPE.getTypeName().equals(comparatorType.getTypeName())) {
			/* composite type */
			try {
				columnModel.setName(new GenericType(StringUtils.split(xmlColumn.getName(), ":"),
						typesBelongingCompositeTypeForComparatorType));
			} catch (IllegalArgumentException e) {
				throw new ParseException(xmlColumn.getName()
						+ " doesn't fit with the schema declaration of your composite type");
			}
		} else {
			/* simple type */
			columnModel.setName(new GenericType(xmlColumn.getName(), GenericTypeEnum.fromValue(comparatorType
					.getTypeName())));
		}

		if (ComparatorType.COUNTERTYPE.getClassName().equals(defaultColumnValueType.getClassName())
				&& TypeExtractor.containFunctions(xmlColumn.getValue())) {
			throw new ParseException("Impossible to override Column value into a Counter column family");
		}

		GenericType columnValue = TypeExtractor.extract(xmlColumn.getValue(), defaultColumnValueType);
		columnModel.setValue(columnValue);

		return columnModel;
	}

	/**
	 * map an xml columns to columns
	 * 
	 * @param xmlColumns
	 *            xml column
	 * @param typesBelongingCompositeTypeForComparatorType
	 * @return columns
	 */
	private List<ColumnModel> mapXmlColumnsToColumnsModel(List<Column> xmlColumns,
			ComparatorType columnNameComparatorType, GenericTypeEnum[] typesBelongingCompositeTypeForComparatorType,
			ComparatorType defaultColumnValueType) {
		List<ColumnModel> columnsModel = new ArrayList<ColumnModel>();
		for (Column xmlColumn : xmlColumns) {
			columnsModel.add(mapXmlColumnToColumnModel(xmlColumn, columnNameComparatorType,
					typesBelongingCompositeTypeForComparatorType, defaultColumnValueType));
		}
		return columnsModel;
	}

	@Override
	public KeyspaceModel getKeyspace() {
		if (keyspace == null) {
			org.cassandraunit.dataset.xml.Keyspace xmlKeyspace = getXmlKeyspace();
			mapXmlKeyspaceToModel(xmlKeyspace);
		}
		return keyspace;
	}

	@Override
	public List<ColumnFamilyModel> getColumnFamilies() {
		if (keyspace == null) {
			getKeyspace();
		}
		return keyspace.getColumnFamilies();
	}

}
