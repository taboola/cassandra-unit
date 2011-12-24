package org.cassandraunit;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.junit.Rule;
import org.junit.Test;

public class CassandraStartAndLoadRuleTest {

	@Rule
	public CassandraUnit cassandra = new CassandraUnit();
	
	@Test
	public void shouldStartCassandraServer ()  {
		assertThat(cassandra.cluster, notNullValue());
	}
}
