package org.cassandraunit;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.cassandraunit.SampleDataSetChecker.assertDataSetLoaded;

import java.util.List;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

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

	private static byte[] decodeHex(String valueToDecode) {
		try {
			return Hex.decodeHex(valueToDecode.toCharArray());
		} catch (DecoderException e) {
			return null;
		}
	}
}
