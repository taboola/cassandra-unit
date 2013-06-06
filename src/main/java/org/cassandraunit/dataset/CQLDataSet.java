package org.cassandraunit.dataset;

import org.cassandraunit.model.ColumnFamilyModel;
import org.cassandraunit.model.KeyspaceModel;

import java.util.List;

/**
 * @author Jeremy Sevellec
 */
public interface CQLDataSet {

    List<String> getCQLQueries();

    String getKeyspaceName();

}
