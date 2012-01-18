package org.cassandraunit.dataset;

import static org.cassandraunit.SampleDataSetChecker.assertDataSetDefaultValues;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
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
	public void shouldNotGetADataSetStructureBecauseOfNull() {
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

}
