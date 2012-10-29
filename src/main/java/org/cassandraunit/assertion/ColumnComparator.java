package org.cassandraunit.assertion;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;
import org.cassandraunit.serializer.GenericTypeSerializer;
import org.cassandraunit.type.GenericType;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

/**
 * Compares data loaded in cassandra against expected datas.
 */
public class ColumnComparator {

    private final Keyspace keyspace;
    private final String columnFamily;

    /**
     * @param keyspace The cassandra Keyspace to query
     * @param columnFamily The column family to query
     */
    public ColumnComparator(Keyspace keyspace, String columnFamily) {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
    }

    /**
     * Verifies that Keyspace contains expected value for expectedRowKey and expectedColumnName
     * @return a List of differences between expected datas and keyspace, empty if expected datas are available (never null)
     */
    public List<Difference> verify(GenericType expectedRowKey, GenericType expectedColumnName, GenericType expectedValue) {
        try {
            ColumnQuery<GenericType,GenericType,GenericType> columnQuery = HFactory.createColumnQuery(
                    keyspace,
                    GenericTypeSerializer.get(expectedRowKey),
                    GenericTypeSerializer.get(expectedColumnName),
                    GenericTypeSerializer.get(expectedValue))
                    .setColumnFamily(columnFamily)
                    .setKey(expectedRowKey)
                    .setName(expectedColumnName);

            HColumn<GenericType, GenericType> queryResult  =
                    columnQuery
                    .execute()
                    .get();
            if (queryResult == null) {
                return asList(new Difference(expectedValue, "No data found for key : " + expectedRowKey + " and column " + expectedColumnName));
            } else if (expectedValue.equals(queryResult.getValue())) {
                return emptyList();
            } else {
                return asList(new Difference(expectedValue, queryResult.getValue()));
            }
        } catch (HInvalidRequestException e) {
            return asList(new Difference(expectedValue, e.getMessage()));
        }
    }
}
