package org.cassandraunit;

import com.datastax.driver.core.Session;
import me.prettyprint.hector.api.Keyspace;
import org.junit.After;
import org.junit.Before;

/**
 * @author Marcin Szymaniuk
 */
public abstract class AbstractCassandraUnit4CQL3DataSetTestCase {

    private CassandraCQLUnit cassandraUnit;
    private Keyspace keyspace = null;
    private boolean initialized = false;
    protected Session session;

    public AbstractCassandraUnit4CQL3DataSetTestCase() {
    }

	@Before
    public void before() throws Exception {
        if (!initialized) {
            cassandraUnit = new CassandraCQLUnit(getCqlFile(),getKeyspaceName());
            cassandraUnit.before();

            keyspace = cassandraUnit.keyspace;

            session = cassandraUnit.createSession();

            initialized = true;

        }

    }
    @After
    public void after(){
        session.shutdown();
    }

    public abstract String getCqlFile();
    public abstract String getKeyspaceName();


    public Keyspace getKeyspace() {
        return keyspace;
    }


}
