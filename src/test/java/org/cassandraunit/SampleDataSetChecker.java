package org.cassandraunit;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.List;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.model.StrategyModel;

public class SampleDataSetChecker {

	public static void assertDataSetLoaded(Keyspace keyspace) {
		RangeSlicesQuery<byte[], byte[], byte[]> query = HFactory.createRangeSlicesQuery(keyspace,
				BytesArraySerializer.get(), BytesArraySerializer.get(), BytesArraySerializer.get());
		query.setColumnFamily("columnFamily1");
		query.setRange(null, null, false, Integer.MAX_VALUE);
		QueryResult<OrderedRows<byte[], byte[], byte[]>> result = query.execute();
		List<Row<byte[], byte[], byte[]>> rows = result.get().getList();
		assertThat(rows.size(), is(3));
		assertThat(rows.get(0).getKey(), is(decodeHex("30")));
		assertThat(rows.get(0).getColumnSlice().getColumns().size(), is(2));
		assertThat(rows.get(0).getColumnSlice().getColumns().get(0).getName(), is(decodeHex("31")));
		assertThat(rows.get(0).getColumnSlice().getColumns().get(0).getValue(), is(decodeHex("31")));
		assertThat(rows.get(0).getColumnSlice().getColumns().get(1).getName(), is(decodeHex("32")));
		assertThat(rows.get(0).getColumnSlice().getColumns().get(1).getValue(), is(decodeHex("32")));
		assertThat(rows.get(1).getKey(), is(decodeHex("10")));
		assertThat(rows.get(2).getKey(), is(decodeHex("20")));

	}

	public static void assertDataSetDefaultValues(DataSet dataSet) {
		assertThat(dataSet, notNullValue());
		assertThat(dataSet.getKeyspace(), notNullValue());
		assertThat(dataSet.getKeyspace().getName(), is("beautifulKeyspaceName"));
		assertThat(dataSet.getKeyspace().getReplicationFactor(), is(1));
		assertThat(dataSet.getKeyspace().getStrategy(), is(StrategyModel.SIMPLE_STRATEGY));

		assertThat(dataSet.getColumnFamilies(), notNullValue());
		assertThat(dataSet.getColumnFamilies().size(), is(1));
		assertThat(dataSet.getColumnFamilies().get(0), notNullValue());
		assertThat(dataSet.getColumnFamilies().get(0).getName(), is("columnFamily1"));
		assertThat(dataSet.getColumnFamilies().get(0).getType(), is(ColumnType.STANDARD));
		assertThat(dataSet.getColumnFamilies().get(0).getKeyType().getTypeName(),
				is(ComparatorType.BYTESTYPE.getTypeName()));
		assertThat(dataSet.getColumnFamilies().get(0).getComparatorType().getTypeName(),
				is(ComparatorType.BYTESTYPE.getTypeName()));
		assertThat(dataSet.getColumnFamilies().get(0).getSubComparatorType(), nullValue());
	}

	private static byte[] decodeHex(String valueToDecode) {
		try {
			return Hex.decodeHex(valueToDecode.toCharArray());
		} catch (DecoderException e) {
			return null;
		}
	}

	public static void assertDefaultValuesSchemaExist(Cluster cluster) {
		assertThat(cluster.describeKeyspace("beautifulKeyspaceName"), notNullValue());
		assertThat(cluster.describeKeyspace("beautifulKeyspaceName").getCfDefs(), notNullValue());
		assertThat(cluster.describeKeyspace("beautifulKeyspaceName").getCfDefs().size(), is(1));
		assertThat(cluster.describeKeyspace("beautifulKeyspaceName").getCfDefs().get(0).getName(),
				is("beautifulColumnFamilyName"));
		assertThat(cluster.describeKeyspace("beautifulKeyspaceName").getCfDefs().get(0).getColumnType(),
				is(ColumnType.STANDARD));
		assertThat(cluster.describeKeyspace("beautifulKeyspaceName").getCfDefs().get(0).getKeyValidationClass(),
				is(ComparatorType.BYTESTYPE.getClassName()));
		assertThat(cluster.describeKeyspace("beautifulKeyspaceName").getCfDefs().get(0).getComparatorType(),
				is(ComparatorType.BYTESTYPE));
		assertThat(cluster.describeKeyspace("beautifulKeyspaceName").getCfDefs().get(0).getKeyValidationClass(),
				is(ComparatorType.BYTESTYPE.getClassName()));
	}
}
