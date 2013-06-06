package org.cassandraunit.dataset.cql;

import org.cassandraunit.dataset.CQLDataSet;
import org.cassandraunit.dataset.ParseException;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Jeremy Sevellec
 */
public class ClasspathCQLDataSetTest {

    @Test
    public void shouldGetACQLDataSet() {

        CQLDataSet dataSet = new ClassPathCQLDataSet("cql/simple.cql");
        assertThat(dataSet, notNullValue());
    }

    @Test
    public void shouldNotGetACQLDataSetBecauseNull() {
        try {
            CQLDataSet dataSet = new ClassPathCQLDataSet(null);
            fail();
        } catch (ParseException e) {
            /* nothing to do, it what we want */
        }
    }

    @Test
    public void shouldNotGetACQLDataSetBecauseItNotExist() {
        try {
            CQLDataSet dataSet = new ClassPathCQLDataSet("cql/unknownDataSet.cql");
            fail();
        } catch (ParseException e) {
            /* nothing to do, it what we want */
        }
    }

    @Test
    public void shouldGetCQLQueries() {
        CQLDataSet dataSet = new ClassPathCQLDataSet("cql/simple.cql");
        assertThat(dataSet.getCQLQueries(), notNullValue());
        assertThat(dataSet.getCQLQueries().isEmpty(), is(false));
        assertThat(dataSet.getCQLQueries().size(),is(4));
        assertThat(dataSet.getCQLQueries().get(0),is("create table testCQLTable (id uuid, value varchar, PRIMARY KEY(id));"));
        assertThat(dataSet.getCQLQueries().get(1),is("insert into testCQLTable(id, value) values(1690e8da-5bf8-49e8-9583-4dff8a570737,'Cql loaded string');"));
        assertThat(dataSet.getCQLQueries().get(2),is("insert into testCQLTable(id, value) values(1690e8da-5bf8-49e8-9583-4dff8a570738,'BLA2');"));
        assertThat(dataSet.getCQLQueries().get(3),is("insert into testCQLTable(id, value) values(1690e8da-5bf8-49e8-9583-4dff8a570739,'BLA1');"));
    }

    @Test
    public void shouldGetDefaultTestKeyspaceName() {
        CQLDataSet dataSet = new ClassPathCQLDataSet("cql/simple.cql");
        assertThat(dataSet.getKeyspaceName(),is("cassandraunittestkeyspace"));
    }

    @Test
    public void shouldGetDefinedTestKeyspaceName() {
        CQLDataSet dataSet = new ClassPathCQLDataSet("cql/simple.cql", "mykeyspace");
        assertThat(dataSet.getKeyspaceName(),is("mykeyspace"));
    }
}
