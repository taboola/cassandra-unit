package org.cassandraunit.cli;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.commons.cli.CommandLine;
import org.junit.Test;

public class CassandraUnitCliLoaderTest {

	public void shouldPrintUsageWhenNoArgumentsSpecified() throws Exception {
		String[] args = {};
		CassandraUnitCliLoader.main(args);
		assertThat(CassandraUnitCliLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldLaunchCliAndGetFileAndGetHostAndPortOptions() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-p", "3160", "-c", "TestCluster" };
		CassandraUnitCliLoader.main(args);
		CommandLine commandLine = CassandraUnitCliLoader.getCommandLine();
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
		CassandraUnitCliLoader.main(args);
		assertThat(CassandraUnitCliLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecauseHostOptionIsMissing() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-p", "3160", "-c", "TestCluster" };
		CassandraUnitCliLoader.main(args);
		assertThat(CassandraUnitCliLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecauseFileOptionIsMissing() throws Exception {
		String[] args = { "-h", "myHost", "-p", "3160", "-c", "TestCluster" };
		CassandraUnitCliLoader.main(args);
		assertThat(CassandraUnitCliLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecauseClusterNameOptionIsMissing() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-p", "3160" };
		CassandraUnitCliLoader.main(args);
		assertThat(CassandraUnitCliLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecauseHostArgumentIsMissing() throws Exception {
		String[] args = { "-h", "-p", "3160" };
		CassandraUnitCliLoader.main(args);
		assertThat(CassandraUnitCliLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecausePortArgumentIsMissing() throws Exception {
		String[] args = { "-h", "myHost", "-p" };
		CassandraUnitCliLoader.main(args);
		assertThat(CassandraUnitCliLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldLaunchCliAndGetOnlySchemaOption() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-p", "3160", "-c", "TestCluster", "-o" };
		CassandraUnitCliLoader.main(args);
		CommandLine commandLine = CassandraUnitCliLoader.getCommandLine();
		assertThat(commandLine.hasOption("o"), is(true));
		assertThat(commandLine.hasOption("onlySchema"), is(true));
	}

	@Test
	public void shouldLaunchCliAndGetReplicationFactorOption() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-p", "3160", "-c", "TestCluster", "-r", "1" };
		CassandraUnitCliLoader.main(args);
		CommandLine commandLine = CassandraUnitCliLoader.getCommandLine();
		assertThat(commandLine.getOptionValue("r"), is("1"));
		assertThat(commandLine.getOptionValue("replicationFactor"), is("1"));
	}

	@Test
	public void shouldPrintUsageBecauseReplicationFactorArgumentIsMissing() throws Exception {
		String[] args = { "-h", "myHost", "-p", "3160", "-r" };
		CassandraUnitCliLoader.main(args);
		assertThat(CassandraUnitCliLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldPrintUsageBecauseReplicationFactorArgumentIsBad() throws Exception {
		String[] args = { "-h", "myHost", "-p", "3160", "-r", "a" };
		CassandraUnitCliLoader.main(args);
		assertThat(CassandraUnitCliLoader.isUsageBeenPrinted(), is(true));
	}

	@Test
	public void shouldLaunchCliAndGetStrategyOption() throws Exception {
		String[] args = { "-f", "dataset.xsd", "-h", "myHost", "-p", "3160", "-c", "TestCluster", "-s",
				"org.apache.cassandra.locator.SimpleStrategy" };
		CassandraUnitCliLoader.main(args);
		CommandLine commandLine = CassandraUnitCliLoader.getCommandLine();
		assertThat(commandLine.getOptionValue("s"), is("org.apache.cassandra.locator.SimpleStrategy"));
		assertThat(commandLine.getOptionValue("strategy"), is("org.apache.cassandra.locator.SimpleStrategy"));
	}

}
