package org.cassandraunit.dataset.cql;

import org.cassandraunit.dataset.CQLDataSet;

import java.io.InputStream;

/**
 * @author Jeremy Sevellec
 */
public class ClassPathCQLDataSet extends AbstractCQLDataSet implements CQLDataSet {

    public ClassPathCQLDataSet(String dataSetLocation) {
        super(dataSetLocation);
    }

    public ClassPathCQLDataSet(String dataSetLocation, String keyspaceName) {
        super(dataSetLocation, keyspaceName);
    }

    @Override
    protected InputStream getInputDataSetLocation(String dataSetLocation) {
        InputStream inputDataSetLocation = this.getClass().getResourceAsStream("/" + dataSetLocation);
        return inputDataSetLocation;
    }
}
