package org.cassandraunit;

import com.datastax.driver.core.Session;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @author Marcin Szymaniuk
 */
public class CassandraCQLUnit extends CassandraUnit {
    public Cluster cluster;
    public Keyspace keyspace;
    public String cqlFile;
    public static String clusterName = "TestCluster";
    public static String host = "localhost:9171";
    public static String keyspaceName = "testkeyspace";

    private static final Logger log = LoggerFactory.getLogger(CassandraCQLUnit.class);
    private String hostIp = "127.0.0.1";
    private int port = 9031;

    public CassandraCQLUnit(String cqlFile, String keyspaceName) {
        super(null);
        this.keyspaceName = keyspaceName;
        this.cqlFile = cqlFile;
    }


    public CassandraCQLUnit(String cqlFile, String keyspaceName, String hostIp, int port) {
        super(null);
        this.keyspaceName = keyspaceName;
        this.cqlFile = cqlFile;
        this.hostIp=hostIp;
        this.port=port;
    }



    protected void load() {
        cluster = HFactory.getOrCreateCluster(clusterName, host);


        dropKeyspaceIfExist(keyspaceName);
        KeyspaceDefinition keyspaceDefinition = new ThriftKsDef(keyspaceName);
        cluster.addKeyspace(keyspaceDefinition, true);
        keyspace = HFactory.createKeyspace(keyspaceName, cluster);

        /* create structure and load data */
        CQLDataLoader dataLoader = new CQLDataLoader(keyspaceName);

        dataLoader.load(cqlFile, createSession());
    }


    private void dropKeyspaceIfExist(String keyspaceName) {
        KeyspaceDefinition existedKeyspace = cluster.describeKeyspace(keyspaceName);
        if (existedKeyspace != null) {
            log.info("dropping existing keyspace : {}", existedKeyspace.getName());
            cluster.dropKeyspace(keyspaceName, true);
        }
    }

    public Session createSession() {

        com.datastax.driver.core.Cluster cluster =
                new com.datastax.driver.core.Cluster.Builder().addContactPoints(hostIp).withPort(port).build();
        Session session = cluster.connect();
        session.execute("USE "+keyspaceName);
        return session;
    }
}
