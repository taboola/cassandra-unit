package org.cassandraunit;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.cassandraunit.dataset.IDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Before;

public abstract class AbstractCassandraUnit4TestCase {

	private Keyspace keyspace = null;
	private boolean initialized = false;
	private Cluster cluster;

	@Before
	public void before() throws Exception {
		if (!initialized) {
			/* start an embedded Cassandra */
			EmbeddedCassandraServerHelper.startEmbeddedCassandra();
			String clusterName = "TestCluster";
			String host = "localhost:9171";

			/* create structure and load data */
			DataLoader dataLoader = new DataLoader(clusterName, host);
			dataLoader.load(getDataSet());

			/* get hector client object to query data in your test */
			cluster = HFactory.getOrCreateCluster(clusterName, host);
			keyspace = HFactory.createKeyspace(getDataSet().getKeyspace().getName(), getCluster());
			initialized = true;
		}

	}

	public abstract IDataSet getDataSet();

	public void setKeyspace(Keyspace keyspace) {
		this.keyspace = keyspace;
	}

	public Keyspace getKeyspace() {
		return keyspace;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public Cluster getCluster() {
		return cluster;
	}

}
