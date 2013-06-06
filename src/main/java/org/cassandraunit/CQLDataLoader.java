package org.cassandraunit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.cassandraunit.dataset.CQLDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Marcin Szymaniuk
 * @author Jeremy Sevellec
 */
public class CQLDataLoader {

    private static final Logger log = LoggerFactory.getLogger(CQLDataLoader.class);

    public Session getSession() {
        return session;
    }

    private final Session session;

    public CQLDataLoader(String hostIp, int port) {
        this.session = createSession(hostIp, port);
    }

    private Session createSession(String hostIp,int port) {
        Cluster cluster =
                new Cluster.Builder().addContactPoints(hostIp).withPort(port).build();
        Session session = cluster.connect();
        return session;
    }


    public void load(CQLDataSet dataSet) {
        initKeyspace(session,dataSet.getKeyspaceName());
        log.debug("loading data");
        for (String query : dataSet.getCQLQueries()) {
            session.execute(query);
        }
    }


    private void initKeyspace(Session session, String keyspaceName) {
        log.debug("initKeyspace " + keyspaceName);
        ResultSet keyspaceQueryResult =
                session.execute("SELECT keyspace_name from system.schema_keyspaces where keyspace_name='" + keyspaceName + "'");
        if(keyspaceQueryResult.iterator().hasNext()){
            session.execute("DROP KEYSPACE "+ keyspaceName);
        }
        session.execute("CREATE KEYSPACE " + keyspaceName + " WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1}");
        session.execute("use " + keyspaceName);
    }
}
