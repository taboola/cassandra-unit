package org.cassandraunit.dataset.json;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.InputStream;

import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.apache.commons.io.FileUtils;
import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.ParseException;
import org.cassandraunit.model.StrategyModel;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class FileJsonDataSetTest {

	private String targetDataSetPathFileName = null;

	@Before
	public void before() throws Exception {
		String dataSetFileName = "dataSetDefaultValues.json";
		InputStream dataSetInputStream = this.getClass().getResourceAsStream("/json/" + dataSetFileName);

		String tmpPath = FileUtils.getTempDirectoryPath() + "/cassandra-unit/dataset/";
		targetDataSetPathFileName = tmpPath + dataSetFileName;
		FileUtils.copyInputStreamToFile(dataSetInputStream, new File(targetDataSetPathFileName));
	}

	@Test
	public void shouldGetAJsonDataSetStructure() throws Exception {

		DataSet dataSet = new FileJsonDataSet(targetDataSetPathFileName);
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
	public void shouldNotGetAJsonDataSetStructureBecauseOfNull() {
		DataSet dataSet = new ClassPathJsonDataSet(null);
		dataSet.getKeyspace();
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAJsonDataSetStructureBecauseOfFileNotFound() {
		DataSet dataSet = new ClassPathJsonDataSet("/notfound.json");
		dataSet.getKeyspace();
	}
}
