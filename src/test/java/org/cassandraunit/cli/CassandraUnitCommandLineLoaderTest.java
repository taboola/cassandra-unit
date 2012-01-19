package org.cassandraunit.cli;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.commons.cli.CommandLine;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.cassandraunit.utils.FileTmpHelper;
import org.junit.Test;

public class CassandraUnitCommandLineLoaderTest {

	public void shouldPrintUsageWhenNoArgumentsSpecified() throws Exception {
		String[] args = {};
		CassandraUnitCommandLineLoader.main(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldLaunchCliAndGetFileAndGetHostAndPortOptions() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-p", "9160" };
		CassandraUnitCommandLineLoader.parseCommandLine(args);
		CommandLine commandLine = CassandraUnitCommandLineLoader.getCommandLine();
		assertThat(commandLine.getOptionValue("f"), is("dataset.xsd"));
		assertThat(commandLine.getOptionValue("file"), is("dataset.xsd"));
		assertThat(commandLine.getOptionValue("h"), is("myHost"));
		assertThat(commandLine.getOptionValue("host"), is("myHost"));
		assertThat(commandLine.getOptionValue("p"), is("9160"));
		assertThat(commandLine.getOptionValue("port"), is("9160"));
	}

	@Test
	public void shouldPrintUsageBecausePortOptionIsMissing() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-c", "TestCluster" };
		CassandraUnitCommandLineLoader.parseCommandLine(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecauseHostOptionIsMissing() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-p", "3160", "-c", "TestCluster" };
		CassandraUnitCommandLineLoader.parseCommandLine(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecauseFileOptionIsMissing() throws Exception {
		String[] args = { "-h", "myHost", "-p", "9160", "-c", "TestCluster" };
		CassandraUnitCommandLineLoader.parseCommandLine(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecauseHostArgumentIsMissing() throws Exception {
		String[] args = { "-h", "-p", "3160" };
		CassandraUnitCommandLineLoader.parseCommandLine(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecausePortArgumentIsMissing() throws Exception {
		String[] args = { "-h", "myHost", "-p" };
		CassandraUnitCommandLineLoader.parseCommandLine(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldLaunchCliAndGetOnlySchemaOption() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-p", "9160", "-o" };
		CassandraUnitCommandLineLoader.parseCommandLine(args);
		CommandLine commandLine = CassandraUnitCommandLineLoader.getCommandLine();
		assertThat(commandLine.hasOption("o"), is(true));
		assertThat(commandLine.hasOption("onlySchema"), is(true));
	}

	@Test
	public void shouldLaunchCliAndGetReplicationFactorOption() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-p", "9160", "-r", "1" };
		CassandraUnitCommandLineLoader.parseCommandLine(args);
		CommandLine commandLine = CassandraUnitCommandLineLoader.getCommandLine();
		assertThat(commandLine.getOptionValue("r"), is("1"));
		assertThat(commandLine.getOptionValue("replicationFactor"), is("1"));
	}

	@Test
	public void shouldPrintUsageBecauseReplicationFactorArgumentIsMissing() throws Exception {
		String[] args = { "-h", "myHost", "-p", "9160", "-r" };
		CassandraUnitCommandLineLoader.parseCommandLine(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecauseReplicationFactorArgumentIsBad() throws Exception {
		String[] args = { "-h", "myHost", "-p", "9160", "-r", "a" };
		CassandraUnitCommandLineLoader.parseCommandLine(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldLaunchCliAndGetStrategyOption() throws Exception {
		String[] args = { "-f", "dataset.xml", "-h", "myHost", "-p", "9160", "-s",
				"org.apache.cassandra.locator.SimpleStrategy" };
		CassandraUnitCommandLineLoader.parseCommandLine(args);
		CommandLine commandLine = CassandraUnitCommandLineLoader.getCommandLine();
		assertThat(commandLine.getOptionValue("s"), is("org.apache.cassandra.locator.SimpleStrategy"));
		assertThat(commandLine.getOptionValue("strategy"), is("org.apache.cassandra.locator.SimpleStrategy"));
	}

	@Test
	public void shouldPrintUsageBecauseStrategyArgumentIsBad() throws Exception {
		String[] args = { "-f", "dataset.xml", "-h", "myHost", "-p", "9160", "-s", "bad" };
		CassandraUnitCommandLineLoader.parseCommandLine(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldLoadDataSet() throws Exception {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra();

		String targetFileDataSet = FileTmpHelper.copyClassPathDataSetToTmpDirectory(this.getClass(),
				"/xml/datasetDefaultValues.xml");
		String clusterName = "TestCluster";
		String host = "localhost";
		String port = "9171";
		String[] args = { "-f", targetFileDataSet, "-h", host, "-p", port };
		CassandraUnitCommandLineLoader.main(args);

		Cluster cluster = HFactory.getOrCreateCluster(clusterName, host + ":" + port);
		KeyspaceDefinition keyspaceDefinition = cluster.describeKeyspace("beautifulKeyspaceName");
		assertThat(keyspaceDefinition, notNullValue());
		assertThat(keyspaceDefinition.getReplicationFactor(), is(1));
		assertThat(keyspaceDefinition.getStrategyClass(), is("org.apache.cassandra.locator.SimpleStrategy"));
		assertThat(keyspaceDefinition.getCfDefs().get(0).getName(), is("columnFamily1"));
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();

	}
}
