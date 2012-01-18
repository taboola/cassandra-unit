package org.cassandraunit.cli;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.commons.cli.CommandLine;
import org.junit.Test;

public class CassandraUnitCommandLineLoaderTest {

	public void shouldPrintUsageWhenNoArgumentsSpecified() throws Exception {
		String[] args = {};
		CassandraUnitCommandLineLoader.main(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldLaunchCliAndGetFileAndGetHostAndPortOptions() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-p", "3160", "-c", "TestCluster" };
		CassandraUnitCommandLineLoader.main(args);
		CommandLine commandLine = CassandraUnitCommandLineLoader.getCommandLine();
		assertThat(commandLine.getOptionValue("f"), is("dataset.xsd"));
		assertThat(commandLine.getOptionValue("file"), is("dataset.xsd"));
		assertThat(commandLine.getOptionValue("h"), is("myHost"));
		assertThat(commandLine.getOptionValue("host"), is("myHost"));
		assertThat(commandLine.getOptionValue("p"), is("3160"));
		assertThat(commandLine.getOptionValue("port"), is("3160"));
		assertThat(commandLine.getOptionValue("c"), is("TestCluster"));
		assertThat(commandLine.getOptionValue("clusterName"), is("TestCluster"));
	}

	@Test
	public void shouldPrintUsageBecausePortOptionIsMissing() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-c", "TestCluster" };
		CassandraUnitCommandLineLoader.main(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecauseHostOptionIsMissing() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-p", "3160", "-c", "TestCluster" };
		CassandraUnitCommandLineLoader.main(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecauseFileOptionIsMissing() throws Exception {
		String[] args = { "-h", "myHost", "-p", "3160", "-c", "TestCluster" };
		CassandraUnitCommandLineLoader.main(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecauseClusterNameOptionIsMissing() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-p", "3160" };
		CassandraUnitCommandLineLoader.main(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecauseHostArgumentIsMissing() throws Exception {
		String[] args = { "-h", "-p", "3160" };
		CassandraUnitCommandLineLoader.main(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecausePortArgumentIsMissing() throws Exception {
		String[] args = { "-h", "myHost", "-p" };
		CassandraUnitCommandLineLoader.main(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldLaunchCliAndGetOnlySchemaOption() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-p", "3160", "-c", "TestCluster", "-o" };
		CassandraUnitCommandLineLoader.main(args);
		CommandLine commandLine = CassandraUnitCommandLineLoader.getCommandLine();
		assertThat(commandLine.hasOption("o"), is(true));
		assertThat(commandLine.hasOption("onlySchema"), is(true));
	}

	@Test
	public void shouldLaunchCliAndGetReplicationFactorOption() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-p", "3160", "-c", "TestCluster", "-r", "1" };
		CassandraUnitCommandLineLoader.main(args);
		CommandLine commandLine = CassandraUnitCommandLineLoader.getCommandLine();
		assertThat(commandLine.getOptionValue("r"), is("1"));
		assertThat(commandLine.getOptionValue("replicationFactor"), is("1"));
	}

	@Test
	public void shouldPrintUsageBecauseReplicationFactorArgumentIsMissing() throws Exception {
		String[] args = { "-h", "myHost", "-p", "3160", "-r" };
		CassandraUnitCommandLineLoader.main(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecauseReplicationFactorArgumentIsBad() throws Exception {
		String[] args = { "-h", "myHost", "-p", "3160", "-r", "a" };
		CassandraUnitCommandLineLoader.main(args);
		assertThat(CassandraUnitCommandLineLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldLaunchCliAndGetStrategyOption() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-p", "3160", "-c", "TestCluster", "-s",
				"org.apache.cassandra.locator.SimpleStrategy" };
		CassandraUnitCommandLineLoader.main(args);
		CommandLine commandLine = CassandraUnitCommandLineLoader.getCommandLine();
		assertThat(commandLine.getOptionValue("s"), is("org.apache.cassandra.locator.SimpleStrategy"));
		assertThat(commandLine.getOptionValue("strategy"), is("org.apache.cassandra.locator.SimpleStrategy"));
	}

}
