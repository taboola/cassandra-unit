package org.cassandraunit.dataset.xml;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.ParseException;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class FileXmlDataSetTest {

	private String targetDataSetPathFileName = null;

	@Before
	public void before() throws Exception {
		String dataSetFileName = "datasetDefaultValues.xml";
		InputStream dataSetInputStream = this.getClass().getResourceAsStream("/xml/" + dataSetFileName);

		String tmpPath = FileUtils.getTempDirectoryPath() + "/cassandra-unit/dataset/";
		targetDataSetPathFileName = tmpPath + dataSetFileName;
		FileUtils.copyInputStreamToFile(dataSetInputStream, new File(targetDataSetPathFileName));
	}

	@Test
	public void shouldGetAXmlDataSet() {

		DataSet dataSet = new FileXmlDataSet(targetDataSetPathFileName);
		assertThat(dataSet, notNullValue());
	}

	@Test
	public void shouldNotGetAXmlDataSetBecauseNull() {
		try {
			DataSet dataSet = new FileXmlDataSet(null);
			fail();
		} catch (ParseException e) {
			/* nothing to do, it what we want */
		}
	}

	@Test
	public void shouldNotGetAXmlDataSetBecauseOfFileNotFound() {
		try {
			DataSet dataSet = new FileXmlDataSet("/notfound.json");
			fail();
		} catch (ParseException e) {
			/* nothing to do, it what we want */
		}
	}
}
