package org.cassandraunit.dataset;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.cassandraunit.model.StrategyModel;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class FileDataSetTest {

	private String targetXmlDataSetPathFileName = null;
	private String targetJsonDataSetPathFileName = null;
	private String targetYamlDataSetPathFileName = null;

	@Before
	public void before() throws Exception {
		targetXmlDataSetPathFileName = copyClassPathDataSetToTmpDirectory("/xml/datasetDefaultValues.xml");
		targetJsonDataSetPathFileName = copyClassPathDataSetToTmpDirectory("/json/dataSetDefaultValues.json");
		targetYamlDataSetPathFileName = copyClassPathDataSetToTmpDirectory("/yaml/dataSetDefaultValues.yaml");
	}

	private String copyClassPathDataSetToTmpDirectory(String initialClasspathDataSetLocation) throws IOException {
		InputStream dataSetInputStream = this.getClass().getResourceAsStream(initialClasspathDataSetLocation);
		String dataSetFileName = StringUtils.substringAfterLast(initialClasspathDataSetLocation, "/");
		String tmpPath = FileUtils.getTempDirectoryPath() + "/cassandra-unit/dataset/";
		String targetDataSetPathFileName = tmpPath + dataSetFileName;
		FileUtils.copyInputStreamToFile(dataSetInputStream, new File(targetDataSetPathFileName));
		return targetDataSetPathFileName;
	}

	@Test
	public void shouldGetAJsonDataSetStructure() throws Exception {

		DataSet dataSet = new FileDataSet(targetJsonDataSetPathFileName);
		assertDataSetDefaultValues(dataSet);
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAJsonDataSetStructureBecauseOfNull() {
		DataSet dataSet = new FileDataSet(null);
		dataSet.getKeyspace();
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAJsonDataSetStructureBecauseOfFileNotFound() {
		DataSet dataSet = new FileDataSet("/notfound.json");
		dataSet.getKeyspace();
	}

	@Test
	public void shouldGetAXmlDataSetStructure() throws Exception {

		DataSet dataSet = new FileDataSet(targetXmlDataSetPathFileName);
		assertDataSetDefaultValues(dataSet);
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAXmlDataSetStructureBecauseOfFileNotFound() {
		DataSet dataSet = new FileDataSet("/notfound.xml");
		dataSet.getKeyspace();
	}

	@Test
	public void shouldGetAYamlDataSetStructure() throws Exception {

		DataSet dataSet = new FileDataSet(targetYamlDataSetPathFileName);
		assertDataSetDefaultValues(dataSet);
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAYamlDataSetStructureBecauseOfFileNotFound() {
		DataSet dataSet = new FileDataSet("/notfound.yaml");
		dataSet.getKeyspace();
	}

	private void assertDataSetDefaultValues(DataSet dataSet) {
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

}
