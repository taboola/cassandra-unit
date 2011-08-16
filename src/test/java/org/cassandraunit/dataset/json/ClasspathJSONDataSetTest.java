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
import org.junit.Test;

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

}
