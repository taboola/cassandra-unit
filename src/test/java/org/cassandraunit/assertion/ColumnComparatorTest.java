package org.cassandraunit.assertion;

import org.cassandraunit.CassandraUnit;
import org.cassandraunit.dataset.json.ClassPathJsonDataSet;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.cassandraunit.type.GenericTypeEnum.BOOLEAN_TYPE;
import static org.cassandraunit.type.GenericTypeEnum.LONG_TYPE;
import static org.cassandraunit.type.GenericTypeEnum.UTF_8_TYPE;
import static org.fest.assertions.Assertions.assertThat;

public class ColumnComparatorTest {

    public static final String EXPECTED_UTF_8_COLUMN_NAME = "expectedUtf8ColumnName";
    public static final String EXPECTED_UTF_8_COLUMN_VALUE = "expectedColumnValue";
    @Rule
    public CassandraUnit cassandraUnit = new CassandraUnit(new ClassPathJsonDataSet("assertion/dataSet.json"));

    @Test
    public void should_find_no_difference_utf8() throws Exception {
        assertThat(verifyUTF8("columnFamily", "key", EXPECTED_UTF_8_COLUMN_NAME, EXPECTED_UTF_8_COLUMN_VALUE)).isEmpty();
    }

    @Test
    public void should_find_no_difference_boolean() throws Exception {
        List<Difference> differences = verify("columnFamily",
                new GenericType("key", UTF_8_TYPE),
                new GenericType(("expectedBooleanColumnName"), UTF_8_TYPE),
                new GenericType("true", BOOLEAN_TYPE));
        assertThat(differences).isEmpty();
    }

    @Test
    public void should_find_one_different_boolean_column_value() throws Exception {
        List<Difference> differences = verify("columnFamily",
                new GenericType("key", UTF_8_TYPE),
                new GenericType(("expectedBooleanColumnName"), UTF_8_TYPE),
                new GenericType("false", BOOLEAN_TYPE));
        assertThat(differences).hasSize(1);
    }

    @Test
    public void should_find_no_difference_composite() throws Exception {
        List<Difference> differences = verify("columnFamilyWithCompositeType",
                new GenericType("key", UTF_8_TYPE),
                new GenericType((new String[] {"12345","foo"}),new GenericTypeEnum[] {LONG_TYPE,UTF_8_TYPE}),
                new GenericType("bar", UTF_8_TYPE));
        assertThat(differences).isEmpty();
    }

    @Test
    public void should_find_one_difference_on_different_utf8_column_values() throws Exception {
        assertThat(verifyUTF8("columnFamily", "key", EXPECTED_UTF_8_COLUMN_NAME, "unexpectedColumnValue")).hasSize(1);
    }

    @Test
    public void should_find_one_difference_with_bad_row_key() throws Exception {
        assertThat(verifyUTF8("columnFamily", "bad_key", EXPECTED_UTF_8_COLUMN_NAME, EXPECTED_UTF_8_COLUMN_VALUE)).hasSize(1);
    }

    @Test
    public void should_find_one_difference_with_bad_column_name() throws Exception {
        assertThat(verifyUTF8("columnFamily", "key", "badColumnName", EXPECTED_UTF_8_COLUMN_VALUE)).hasSize(1);
    }

    @Test
    public void should_find_one_difference_with_bad_column_family() throws Exception {
        List<Difference> differences = verifyUTF8("badColumnFamily", "key", "badColumnName", EXPECTED_UTF_8_COLUMN_VALUE);
        assertThat(differences).hasSize(1);
    }

    @Test
    public void should_find_one_difference_with_bad_column_value_type() throws Exception {
        GenericType expectedKey = new GenericType("key", UTF_8_TYPE);
        GenericType expectedColumnName = new GenericType(EXPECTED_UTF_8_COLUMN_NAME, UTF_8_TYPE);
        GenericType expectedColumnValue = new GenericType(EXPECTED_UTF_8_COLUMN_VALUE, BOOLEAN_TYPE);
        ColumnComparator comparator = new ColumnComparator(cassandraUnit.keyspace, "columnFamily");

        List<Difference> differences = comparator.verify(expectedKey, expectedColumnName, expectedColumnValue);
        assertThat(differences).hasSize(1);
    }

    private List<Difference> verifyUTF8(String columnFamily, String rowKey, String columnName, String columnValue) {
        GenericType expectedColumnValue = new GenericType(columnValue, UTF_8_TYPE);
        GenericType expectedKey = new GenericType(rowKey, UTF_8_TYPE);
        GenericType expectedColumnName = new GenericType(columnName, UTF_8_TYPE);
        return verify(columnFamily, expectedKey, expectedColumnName, expectedColumnValue);
    }

    private List<Difference> verify(String columnFamily, GenericType expectedKey, GenericType expectedColumnName, GenericType expectedColumnValue) {
        ColumnComparator comparator = new ColumnComparator(cassandraUnit.keyspace, columnFamily);
        return comparator.verify(expectedKey, expectedColumnName, expectedColumnValue);
    }
}
