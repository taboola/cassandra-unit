package org.cassandraunit.dataset.cql;

import org.cassandraunit.dataset.CQLDataSet;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * @author Jeremy Sevellec
 */
public class FileCQLDataSet extends AbstractCQLDataSet implements CQLDataSet {

    public FileCQLDataSet(String dataSetLocation) {
        super(dataSetLocation);
    }

    public FileCQLDataSet(String dataSetLocation, String keyspaceName) {
        super(dataSetLocation,keyspaceName);
    }

    @Override
    protected InputStream getInputDataSetLocation(String dataSetLocation) {
        if (dataSetLocation == null) {
            return null;
        }
        try {
            return new FileInputStream(dataSetLocation);
        } catch (FileNotFoundException e) {
            return null;
        }
    }
}
