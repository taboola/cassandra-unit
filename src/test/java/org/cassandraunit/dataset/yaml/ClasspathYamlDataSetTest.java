package org.cassandraunit.dataset.yaml;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.ParseException;
import org.cassandraunit.model.StrategyModel;
import org.cassandraunit.type.GenericTypeEnum;
import org.junit.Test;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class ClasspathYamlDataSetTest {

	@Test
	public void shouldGetAYamlDataSetStructure() {
		DataSet dataSet = new ClassPathYamlDataSet("yaml/dataSetDefaultValues.yaml");
		assertThat(dataSet, notNullValue());
		assertThat(dataSet.getKeyspace(), notNullValue());
		assertThat(dataSet.getKeyspace().getName(), is("beautifulKeyspaceName"));
		assertThat(dataSet.getKeyspace().getReplicationFactor(), is(1));
		assertThat(dataSet.getKeyspace().getStrategy(), is(StrategyModel.SIMPLE_STRATEGY));

		assertThat(dataSet.getColumnFamilies(), notNullValue());
		assertThat(dataSet.getColumnFamilies().size(), is(1));
		assertThat(dataSet.getColumnFamilies().get(0), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(0).getName(), is("columnFamily1"));
		assertThat(dataSet.getColumnFamilies().get(0).getType(), is(ColumnType.STANDARD));
		assertThat(dataSet.getColumnFamilies().get(0).getKeyType().getTypeName(),
				is(ComparatorType.BYTESTYPE.getTypeName()));
		assertThat(dataSet.getColumnFamilies().get(0).getComparatorType().getTypeName(),
				is(ComparatorType.BYTESTYPE.getTypeName()));
		assertThat(dataSet.getColumnFamilies().get(0).getSubComparatorType(), nullValue());
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAYamlDataSetStructureBecauseOfNull() {
		DataSet dataSet = new ClassPathYamlDataSet(null);
		dataSet.getKeyspace();
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAYamlDataSetStructureBecauseOfDataSetNotExist() {
		DataSet dataSet = new ClassPathYamlDataSet("yaml/unknown.yaml");
		dataSet.getKeyspace();
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAYamlDataSetStructureBecauseOfDataSetEmpty() {
		DataSet dataSet = new ClassPathYamlDataSet("yaml/dataSetBadMissingKeyspaceName.yaml");
		dataSet.getKeyspace();
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAYamlDataSetStructureBecauseOfMissingKeyspaceName() {
		DataSet dataSet = new ClassPathYamlDataSet("yaml/dataSetBadMissingKeyspaceName.yaml");
		dataSet.getKeyspace();
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAYamlDataSetStructureBecauseOfInvalidStrategy() {
		DataSet dataSet = new ClassPathYamlDataSet("yaml/dataSetBadUnknownStrategy.yaml");
		dataSet.getKeyspace();
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAYamlDataSetStructureBecauseOfMissingColumnFamilyName() {
		DataSet dataSet = new ClassPathYamlDataSet("yaml/dataSetBadMissingColumnFamilyName.yaml");
		dataSet.getKeyspace();
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAYamlDataSetStructureBecauseOfInvalidColumnFamilyType() {
		DataSet dataSet = new ClassPathYamlDataSet("yaml/dataSetBadInvalidColumnFamilyType.yaml");
		dataSet.getKeyspace();
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAYamlDataSetStructureBecauseOfInvalidRowKeyType() {
		DataSet dataSet = new ClassPathYamlDataSet("yaml/dataSetBadInvalidKeyType.yaml");
		dataSet.getKeyspace();
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAYamlDataSetStructureBecauseOfInvalidComparatorType() {
		DataSet dataSet = new ClassPathYamlDataSet("yaml/dataSetBadInvalidComparatorType.yaml");
		dataSet.getKeyspace();
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAYamlDataSetStructureBecauseOfInvalidSubComparatorType() {
		DataSet dataSet = new ClassPathYamlDataSet("yaml/dataSetBadInvalidSubComparatorType.yaml");
		dataSet.getKeyspace();
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAYamlDataSetStructureBecauseOfInvalidDefaultColumnValueType() {
		DataSet dataSet = new ClassPathYamlDataSet("yaml/dataSetBadInvalidDefaultColumnValueType.yaml");
		dataSet.getKeyspace();
	}

	@Test
	public void shouldGetAYamlDataSetWithData() {
		DataSet dataSet = new ClassPathYamlDataSet("yaml/dataSetDefaultValues.yaml");
		assertThat(dataSet, notNullValue());
		assertThat(dataSet.getKeyspace(), notNullValue());

		assertThat(dataSet.getColumnFamilies(), notNullValue());
		assertThat(dataSet.getColumnFamilies().size(), is(1));
		assertThat(dataSet.getColumnFamilies().get(0), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(0).getName(), is("columnFamily1"));
		assertThat(dataSet.getColumnFamilies().get(0).getRows(), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(0).getRows().size(), is(1));
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getKey().getValue(), is("key01"));
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getKey().getType(),
				is(GenericTypeEnum.BYTES_TYPE));
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getColumns(), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getColumns().size(), is(1));
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getColumns().get(0), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getColumns().get(0).getName().getValue(),
				is("columnName1"));
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getColumns().get(0).getName().getType(),
				is(GenericTypeEnum.BYTES_TYPE));
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getColumns().get(0).getValue().getValue(),
				is("columnValue1"));
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getColumns().get(0).getValue().getType(),
				is(GenericTypeEnum.BYTES_TYPE));

	}

	@Test
	public void shouldGetAYamlDataSetWithDefinedValuesAndData() {
		DataSet dataSet = new ClassPathYamlDataSet("yaml/dataSetDefinedValues.yaml");
		assertThat(dataSet, notNullValue());
		assertThat(dataSet.getKeyspace(), notNullValue());
		assertThat(dataSet.getKeyspace().getName(), is("beautifulDefinedKeyspaceName"));
		assertThat(dataSet.getKeyspace().getReplicationFactor(), is(1));
		assertThat(dataSet.getKeyspace().getStrategy(), is(StrategyModel.SIMPLE_STRATEGY));

		assertThat(dataSet.getColumnFamilies(), notNullValue());
		assertThat(dataSet.getColumnFamilies().size(), is(2));

		assertThat(dataSet.getColumnFamilies().get(0), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(0).getName(), is("columnFamily1"));
		assertThat(dataSet.getColumnFamilies().get(0).getKeyType().getTypeName(),
				is(ComparatorType.UTF8TYPE.getTypeName()));
		assertThat(dataSet.getColumnFamilies().get(0).getComparatorType().getTypeName(),
				is(ComparatorType.UTF8TYPE.getTypeName()));
		assertThat(dataSet.getColumnFamilies().get(0).getSubComparatorType(), is(nullValue()));

		assertThat(dataSet.getColumnFamilies().get(0).getRows(), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(0).getRows().size(), is(1));
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getKey().getValue(), is("key01"));
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getKey().getType(),
				is(GenericTypeEnum.UTF_8_TYPE));
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getColumns(), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getColumns().size(), is(1));
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getColumns().get(0), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getColumns().get(0).getName().getValue(),
				is("columnName1"));
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getColumns().get(0).getName().getType(),
				is(GenericTypeEnum.UTF_8_TYPE));
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getColumns().get(0).getValue().getValue(),
				is("columnValue1"));
		assertThat(dataSet.getColumnFamilies().get(0).getRows().get(0).getColumns().get(0).getValue().getType(),
				is(GenericTypeEnum.UTF_8_TYPE));

		assertThat(dataSet.getColumnFamilies().get(1), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(1).getName(), is("columnFamily2"));
		assertThat(dataSet.getColumnFamilies().get(1).getKeyType().getTypeName(),
				is(ComparatorType.UTF8TYPE.getTypeName()));
		assertThat(dataSet.getColumnFamilies().get(1).getComparatorType().getTypeName(),
				is(ComparatorType.UTF8TYPE.getTypeName()));
		assertThat(dataSet.getColumnFamilies().get(1).getSubComparatorType().getTypeName(),
				is(ComparatorType.UTF8TYPE.getTypeName()));

		assertThat(dataSet.getColumnFamilies().get(1).getRows(), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(1).getRows().size(), is(1));
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getKey().getValue(), is("key02"));
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getKey().getType(),
				is(GenericTypeEnum.UTF_8_TYPE));
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getColumns(), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getColumns().isEmpty(), is(true));
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getSuperColumns(), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getSuperColumns().size(), is(1));
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getSuperColumns().get(0), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getSuperColumns().get(0).getName().getValue(),
				is("superColumnName2"));
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getSuperColumns().get(0).getName().getType(),
				is(GenericTypeEnum.UTF_8_TYPE));
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getSuperColumns().get(0).getColumns(),
				notNullValue());
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getSuperColumns().get(0).getColumns().size(),
				is(1));
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getSuperColumns().get(0).getColumns().get(0),
				notNullValue());
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getSuperColumns().get(0).getColumns().get(0)
				.getName().getValue(), is("columnName2"));
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getSuperColumns().get(0).getColumns().get(0)
				.getName().getType(), is(GenericTypeEnum.UTF_8_TYPE));
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getSuperColumns().get(0).getColumns().get(0)
				.getValue().getValue(), is("2"));
		assertThat(dataSet.getColumnFamilies().get(1).getRows().get(0).getSuperColumns().get(0).getColumns().get(0)
				.getValue().getType(), is(GenericTypeEnum.LONG_TYPE));

	}
}
