package org.cassandraunit;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.List;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.xml.ClassPathXmlDataSet;
import org.junit.Test;

public class CassandraStartAndLoadTest extends AbstractCassandraUnit4TestCase {

	@Override
	public DataSet getDataSet() {
		return new ClassPathXmlDataSet("datasetDefaultValues.xml");
	}

	@Test
	public void shouldWork() throws Exception {
		assertThat(getKeyspace(), notNullValue());
		queryAndVerify();
	}

	@Test
	public void shouldWorkToo() throws Exception {
		assertThat(getKeyspace(), notNullValue());
		queryAndVerify();
	}

	private void queryAndVerify() {
		RangeSlicesQuery<byte[], byte[], byte[]> query = HFactory.createRangeSlicesQuery(getKeyspace(),
				BytesArraySerializer.get(), BytesArraySerializer.get(), BytesArraySerializer.get());
		query.setColumnFamily("beautifulColumnFamilyName");
		query.setRange(null, null, false, Integer.MAX_VALUE);
		QueryResult<OrderedRows<byte[], byte[], byte[]>> result = query.execute();
		List<Row<byte[], byte[], byte[]>> rows = result.get().getList();
		assertThat(rows.size(), is(3));
		assertThat(rows.get(0).getKey(), is("key30".getBytes()));
		assertThat(rows.get(0).getColumnSlice().getColumns().size(), is(2));
		assertThat(rows.get(0).getColumnSlice().getColumns().get(0).getName(), is("name31".getBytes()));
		assertThat(rows.get(0).getColumnSlice().getColumns().get(0).getValue(), is("value31".getBytes()));
		assertThat(rows.get(0).getColumnSlice().getColumns().get(1).getName(), is("name32".getBytes()));
		assertThat(rows.get(0).getColumnSlice().getColumns().get(1).getValue(), is("value32".getBytes()));
		assertThat(rows.get(1).getKey(), is("key20".getBytes()));
		assertThat(rows.get(2).getKey(), is("key10".getBytes()));
	}

}
