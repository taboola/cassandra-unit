package org.cassandraunit.dataset.json;

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
public class ClasspathJSONDataSetTest {

	@Test
	public void shouldGetAJSONDataSetStructure() {
		DataSet dataSet = new ClassPathJSONDataSet("dataSetDefaultValues.json");
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
	public void shouldNotGetAJSONDataSetStructureBecauseOfDataSetNotExist() {
		DataSet dataSet = new ClassPathJSONDataSet("unknown.json");
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAJSONDataSetStructureBecauseOfMissingKeyspaceName() {
		DataSet dataSet = new ClassPathJSONDataSet("dataSetBadMissingKeyspaceName.json");
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAJSONDataSetStructureBecauseOfInvalidStrategy() {
		DataSet dataSet = new ClassPathJSONDataSet("dataSetBadUnknownStrategy.json");
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAJSONDataSetStructureBecauseOfMissingColumnFamilyName() {
		DataSet dataSet = new ClassPathJSONDataSet("dataSetBadMissingColumnFamilyName.json");
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAJSONDataSetStructureBecauseOfInvalidColumnFamilyType() {
		DataSet dataSet = new ClassPathJSONDataSet("dataSetBadInvalidColumnFamilyType.json");
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAJSONDataSetStructureBecauseOfInvalidRowKeyType() {
		DataSet dataSet = new ClassPathJSONDataSet("dataSetBadInvalidKeyType.json");
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAJSONDataSetStructureBecauseOfInvalidComparatorType() {
		DataSet dataSet = new ClassPathJSONDataSet("dataSetBadInvalidComparatorType.json");
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAJSONDataSetStructureBecauseOfInvalidSubComparatorType() {
		DataSet dataSet = new ClassPathJSONDataSet("dataSetBadInvalidSubComparatorType.json");
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAJSONDataSetStructureBecauseOfInvalidDefaultColumnValueType() {
		DataSet dataSet = new ClassPathJSONDataSet("dataSetBadInvalidDefaultColumnValueType.json");
	}

	@Test
	public void shouldGetAJSONDataSetWithData() {
		DataSet dataSet = new ClassPathJSONDataSet("dataSetDefaultValues.json");
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
	public void shouldGetAJSONDataSetWithDefinedValuesAndData() {
		DataSet dataSet = new ClassPathJSONDataSet("dataSetDefinedValues.json");
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
