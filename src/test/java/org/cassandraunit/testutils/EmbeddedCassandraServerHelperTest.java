package org.cassandraunit.testutils;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Test;

public class EmbeddedCassandraServerHelperTest {

	@Test
	public void shouldStartAndCleanAnEmbeddedCassandra() throws Exception {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra();
		testIfTheEmbeddedCassandraServerIsUp();
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
		EmbeddedCassandraServerHelper.startEmbeddedCassandra();
		testIfTheEmbeddedCassandraServerIsUp();
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();

	}

	private void testIfTheEmbeddedCassandraServerIsUp() {
		Cluster cluster = HFactory.getOrCreateCluster("TestCluster", new CassandraHostConfigurator("127.0.0.1:9171"));
		assertThat(cluster.getConnectionManager().getActivePools().size(), is(1));
		KeyspaceDefinition keyspaceDefinition = cluster.describeKeyspace("system");
		assertThat(keyspaceDefinition, notNullValue());
		assertThat(keyspaceDefinition.getReplicationFactor(), is(1));
	}
}
