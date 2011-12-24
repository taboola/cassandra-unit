package org.cassandraunit;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.rules.ExternalResource;

public class CassandraUnit extends ExternalResource {
	public Cluster cluster;
	public String clusterName = "TestCluster";
	public String host = "localhost:9171";

	@Override
	protected void before() throws Exception {
		/* start an embedded Cassandra */
		EmbeddedCassandraServerHelper.startEmbeddedCassandra();

		/* get hector client object to query data in your test */
		cluster = HFactory.getOrCreateCluster(clusterName, host);
	}

	
	
}
