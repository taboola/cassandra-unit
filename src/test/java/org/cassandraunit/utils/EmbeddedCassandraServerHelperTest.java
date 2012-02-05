package org.cassandraunit.utils;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Test;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class EmbeddedCassandraServerHelperTest {

	@Test
	public void shouldStartAndCleanAnEmbeddedCassandra() throws Exception {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra();
		testIfTheEmbeddedCassandraServerIsUpOnHost("127.0.0.1:9171");
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
		EmbeddedCassandraServerHelper.startEmbeddedCassandra();
		testIfTheEmbeddedCassandraServerIsUpOnHost("127.0.0.1:9171");
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
		EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
	}

	@Test
	public void shouldStartTheEmbeddedCassandraServerWithAnotherCassandraYamlConf() throws Exception {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("another-cassandra.yaml");
		testIfTheEmbeddedCassandraServerIsUpOnHost("127.0.0.1:9175");
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
		EmbeddedCassandraServerHelper.stopEmbeddedCassandra();

	}

	private void testIfTheEmbeddedCassandraServerIsUpOnHost(String hostAndPort) {
		Cluster cluster = HFactory.getOrCreateCluster("TestCluster", new CassandraHostConfigurator(hostAndPort));
		assertThat(cluster.getConnectionManager().getActivePools().size(), is(1));
		KeyspaceDefinition keyspaceDefinition = cluster.describeKeyspace("system");
		assertThat(keyspaceDefinition, notNullValue());
		assertThat(keyspaceDefinition.getReplicationFactor(), is(1));
	}

}
