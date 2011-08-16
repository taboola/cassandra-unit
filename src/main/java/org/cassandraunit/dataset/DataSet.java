package org.cassandraunit.dataset;

import java.util.List;

import org.cassandraunit.model.ColumnFamilyModel;
import org.cassandraunit.model.KeyspaceModel;


public interface DataSet {

	KeyspaceModel getKeyspace();

	List<ColumnFamilyModel> getColumnFamilies();

}
