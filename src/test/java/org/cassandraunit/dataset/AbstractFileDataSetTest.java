package org.cassandraunit.dataset;

import java.io.File;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;

public abstract class AbstractFileDataSetTest {

	protected String targetDataSetPathFileName = null;

	@Before
	public void before() throws Exception {
		// String dataSetFileName = "dataSetDefaultValues.json";
		String dataSetFileName = getFileNameFromDatSetClassPathResource(getDataSetClasspathRessource());
		InputStream dataSetInputStream = this.getClass().getResourceAsStream(getDataSetClasspathRessource());

		String tmpPath = FileUtils.getTempDirectoryPath() + "/cassandra-unit/dataset/";
		targetDataSetPathFileName = tmpPath + dataSetFileName;
		FileUtils.copyInputStreamToFile(dataSetInputStream, new File(targetDataSetPathFileName));
	}

	private String getFileNameFromDatSetClassPathResource(String dataSetClasspathRessource) {
		StringUtils.substringAfterLast(dataSetClasspathRessource, "/");
		return null;
	}

	public abstract String getDataSetClasspathRessource();

}
