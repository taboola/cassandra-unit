package org.cassandraunit.serializer;

import me.prettyprint.cassandra.serializers.AsciiSerializer;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.FloatSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.beans.Composite;
import org.apache.commons.codec.binary.Hex;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import static org.cassandraunit.type.GenericTypeEnum.*;
import static org.fest.assertions.Assertions.assertThat;

public class GenericTypeSerializerTest {

    @Test
    public void should_deserialize_all_types() throws Exception {
        for (GenericTypeEnum type : values()) {
            if (type == COMPOSITE_TYPE) {
                continue;
            }
            try {
                GenericTypeSerializer.get(type).fromByteBuffer(null);
            } catch (NullPointerException e) {
                // good
            }
        }
    }

    @Test
    public void should_serialize_ascii_type() throws Exception {
        String sourceValue = "azerty";
        GenericType genericType = new GenericType(sourceValue, ASCII_TYPE);
        ByteBuffer byteBuffer = GenericTypeSerializer.get(genericType).toByteBuffer(genericType);
        String actualValue = StringSerializer.get().fromByteBuffer(byteBuffer);
        assertThat(actualValue).isEqualTo(sourceValue);
    }

    @Test
    public void should_deserialize_ascii_type() throws Exception {
        String value = "azerty";
        ByteBuffer byteBuffer = AsciiSerializer.get().toByteBuffer(value);
        GenericType genericType = GenericTypeSerializer.get(ASCII_TYPE).fromByteBuffer(byteBuffer);
        assertThat(genericType.getValue()).isEqualTo(value);
        assertThat(genericType.getType()).isEqualTo(ASCII_TYPE);
    }

    @Test
    public void should_serialize_bytes_type() throws Exception {
        String sourceValue = "369ff963196dc2e5fe174dad2c0c6e9149b1acd9";
        GenericType genericType = new GenericType(sourceValue, BYTES_TYPE);
        ByteBuffer byteBuffer = GenericTypeSerializer.get(genericType).toByteBuffer(genericType);
        String actualValue = new String(Hex.encodeHex(BytesArraySerializer.get().fromByteBuffer(byteBuffer)));
        assertThat(actualValue).isEqualTo(sourceValue);
    }

    @Test
    public void should_deserialize_bytes_type() throws Exception {
        String value = "369ff963196dc2e5fe174dad2c0c6e9149b1acd9";
        ByteBuffer byteBuffer = BytesArraySerializer.get().toByteBuffer(Hex.decodeHex(value.toCharArray()));
        GenericType genericType = GenericTypeSerializer.get(BYTES_TYPE).fromByteBuffer(byteBuffer);
        assertThat(genericType.getValue()).isEqualTo(value);
        assertThat(genericType.getType()).isEqualTo(BYTES_TYPE);
    }

    @Test
    public void should_serialize_boolean_type() throws Exception {
        Boolean sourceValue = Boolean.FALSE;
        GenericType genericType = new GenericType(sourceValue.toString(), BOOLEAN_TYPE);
        ByteBuffer byteBuffer = GenericTypeSerializer.get(genericType).toByteBuffer(genericType);
        Boolean actualValue = BooleanSerializer.get().fromByteBuffer(byteBuffer);
        assertThat(actualValue).isEqualTo(sourceValue);
    }

    @Test
    public void should_deserialize_boolean_type() throws Exception {
        String value = "true";
        ByteBuffer byteBuffer = BooleanSerializer.get().toByteBuffer(Boolean.parseBoolean(value));
        GenericType genericType = GenericTypeSerializer.get(BOOLEAN_TYPE).fromByteBuffer(byteBuffer);
        assertThat(genericType.getValue()).isEqualTo(value);
        assertThat(genericType.getType()).isEqualTo(BOOLEAN_TYPE);
    }

    @Test
    public void should_serialize_counter_column_type() throws Exception {
        Long sourceValue = 766767L;
        GenericType genericType = new GenericType(sourceValue.toString(), COUNTER_TYPE);
        ByteBuffer byteBuffer = GenericTypeSerializer.get(genericType).toByteBuffer(genericType);
        Long actualValue = LongSerializer.get().fromByteBuffer(byteBuffer);
        assertThat(actualValue).isEqualTo(sourceValue);
    }

    @Test
    public void should_deserialize_counter_column_type() throws Exception {
        String value = "5";
        ByteBuffer byteBuffer = LongSerializer.get().toByteBuffer(Long.parseLong(value));
        GenericType genericType = GenericTypeSerializer.get(COUNTER_TYPE).fromByteBuffer(byteBuffer);
        assertThat(genericType.getValue()).isEqualTo(value);
        assertThat(genericType.getType()).isEqualTo(COUNTER_TYPE);
    }

    @Test
    public void should_serialize_date_type() throws Exception {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MILLISECOND, 0);
        Date sourceValue = cal.getTime();
        GenericType genericType = new GenericType(GenericTypeSerializer.dateFormat.format(sourceValue), DATE_TYPE);
        ByteBuffer byteBuffer = GenericTypeSerializer.get(genericType).toByteBuffer(genericType);
        Date actualValue = DateSerializer.get().fromByteBuffer(byteBuffer);
        assertThat(actualValue).isEqualTo(sourceValue);
    }

    @Test
    public void should_deserialize_date_type() throws Exception {
        String value = "20011231 235959";
        Date date = GenericTypeSerializer.dateFormat.parse(value);
        ByteBuffer byteBuffer = DateSerializer.get().toByteBuffer(date);
        GenericType genericType = GenericTypeSerializer.get(DATE_TYPE).fromByteBuffer(byteBuffer);
        assertThat(genericType.getValue()).isEqualTo(value);
        assertThat(genericType.getType()).isEqualTo(DATE_TYPE);
    }

    @Test
    public void should_serialize_double_type() throws Exception {
        Double sourceValue = 6775.0d;
        GenericType genericType = new GenericType(sourceValue.toString(), DOUBLE_TYPE);
        ByteBuffer byteBuffer = GenericTypeSerializer.get(genericType).toByteBuffer(genericType);
        Double actualValue = DoubleSerializer.get().fromByteBuffer(byteBuffer);
        assertThat(actualValue).isEqualTo(sourceValue);
    }

    @Test
    public void should_deserialize_double_type() throws Exception {
        String value = "123.0";
        ByteBuffer byteBuffer = DoubleSerializer.get().toByteBuffer(Double.valueOf(value));
        GenericType genericType = GenericTypeSerializer.get(DOUBLE_TYPE).fromByteBuffer(byteBuffer);
        assertThat(genericType.getValue()).isEqualTo(value);
        assertThat(genericType.getType()).isEqualTo(DOUBLE_TYPE);
    }

    @Test
    public void should_serialize_float_type() throws Exception {
        Float sourceValue = 6775.0f;
        GenericType genericType = new GenericType(sourceValue.toString(), FLOAT_TYPE);
        ByteBuffer byteBuffer = GenericTypeSerializer.get(genericType).toByteBuffer(genericType);
        Float actualValue = FloatSerializer.get().fromByteBuffer(byteBuffer);
        assertThat(actualValue).isEqualTo(sourceValue);
    }

    @Test
    public void should_deserialize_float_type() throws Exception {
        String value = "123.0";
        ByteBuffer byteBuffer = FloatSerializer.get().toByteBuffer(Float.valueOf(value));
        GenericType genericType = GenericTypeSerializer.get(FLOAT_TYPE).fromByteBuffer(byteBuffer);
        assertThat(genericType.getValue()).isEqualTo(value);
        assertThat(genericType.getType()).isEqualTo(FLOAT_TYPE);
    }

    @Test
    public void should_serialize_integer_type() throws Exception {
        Integer sourceValue = 6775;
        GenericType genericType = new GenericType(sourceValue.toString(), INTEGER_TYPE);
        ByteBuffer byteBuffer = GenericTypeSerializer.get(genericType).toByteBuffer(genericType);
        Integer actualValue = IntegerSerializer.get().fromByteBuffer(byteBuffer);
        assertThat(actualValue).isEqualTo(sourceValue);
    }

    @Test
    public void should_deserialize_integer_type() throws Exception {
        String value = "123";
        ByteBuffer byteBuffer = IntegerSerializer.get().toByteBuffer(Integer.valueOf(value));
        GenericType genericType = GenericTypeSerializer.get(INTEGER_TYPE).fromByteBuffer(byteBuffer);
        assertThat(genericType.getValue()).isEqualTo(value);
        assertThat(genericType.getType()).isEqualTo(INTEGER_TYPE);
    }

    @Test
    public void should_serialize_lexical_uuid_type() throws Exception {
        UUID sourceValue = UUID.randomUUID();
        GenericType genericType = new GenericType(sourceValue.toString(), LEXICAL_UUID_TYPE);
        ByteBuffer byteBuffer = GenericTypeSerializer.get(genericType).toByteBuffer(genericType);
        UUID actualValue = UUIDSerializer.get().fromByteBuffer(byteBuffer);
        assertThat(actualValue).isEqualTo(sourceValue);
    }

    @Test
    public void should_deserialize_lexical_uuid_type() throws Exception {
        String value = "de80a290-dd71-11e0-8f9d-f81edfd6bd91";
        ByteBuffer byteBuffer = UUIDSerializer.get().toByteBuffer(UUID.fromString(value));
        GenericType genericType = GenericTypeSerializer.get(LEXICAL_UUID_TYPE).fromByteBuffer(byteBuffer);
        assertThat(genericType.getValue()).isEqualTo(value);
        assertThat(genericType.getType()).isEqualTo(LEXICAL_UUID_TYPE);
    }

    @Test
    public void should_serialize_long_type() throws Exception {
        Long sourceValue = 766767L;
        GenericType genericType = new GenericType(sourceValue.toString(), LONG_TYPE);
        ByteBuffer byteBuffer = GenericTypeSerializer.get(genericType).toByteBuffer(genericType);
        Long actualValue = LongSerializer.get().fromByteBuffer(byteBuffer);
        assertThat(actualValue).isEqualTo(sourceValue);
    }

    @Test
    public void should_deserialize_long_type() throws Exception {
        String value = "123456";
        ByteBuffer byteBuffer = LongSerializer.get().toByteBuffer(Long.valueOf(value));
        GenericType genericType = GenericTypeSerializer.get(LONG_TYPE).fromByteBuffer(byteBuffer);
        assertThat(genericType.getValue()).isEqualTo(value);
        assertThat(genericType.getType()).isEqualTo(LONG_TYPE);
    }

    @Test
    public void should_serialize_time_uuid_type() throws Exception {
        UUID sourceValue = UUID.randomUUID();
        GenericType genericType = new GenericType(sourceValue.toString(), TIME_UUID_TYPE);
        ByteBuffer byteBuffer = GenericTypeSerializer.get(genericType).toByteBuffer(genericType);
        UUID actualValue = UUIDSerializer.get().fromByteBuffer(byteBuffer);
        assertThat(actualValue).isEqualTo(sourceValue);
    }

    @Test
    public void should_deserialize_time_uuid_type() throws Exception {
        String value = "a4a70900-24e1-11df-8924-001ff3591711";
        ByteBuffer byteBuffer = UUIDSerializer.get().toByteBuffer(UUID.fromString(value));
        GenericType genericType = GenericTypeSerializer.get(TIME_UUID_TYPE).fromByteBuffer(byteBuffer);
        assertThat(genericType.getValue()).isEqualTo(value);
        assertThat(genericType.getType()).isEqualTo(TIME_UUID_TYPE);
    }

    @Test
    public void should_serialize_utf8_type() throws Exception {
        String sourceValue = "éè";
        GenericType genericType = new GenericType(sourceValue.toString(), UTF_8_TYPE);
        ByteBuffer byteBuffer = GenericTypeSerializer.get(genericType).toByteBuffer(genericType);
        String actualValue = StringSerializer.get().fromByteBuffer(byteBuffer);
        assertThat(actualValue).isEqualTo(sourceValue);
    }


    @Test
    public void should_deserialize_utf8_type() throws Exception {
        String value = "fooù";
        ByteBuffer byteBuffer = StringSerializer.get().toByteBuffer(value);
        GenericType genericType = GenericTypeSerializer.get(UTF_8_TYPE).fromByteBuffer(byteBuffer);
        assertThat(genericType.getValue()).isEqualTo(value);
        assertThat(genericType.getType()).isEqualTo(UTF_8_TYPE);
    }

    @Test
    public void should_serialize_uuid_type() throws Exception {
        UUID sourceValue = UUID.randomUUID();
        GenericType genericType = new GenericType(sourceValue.toString(), UUID_TYPE);
        ByteBuffer byteBuffer = GenericTypeSerializer.get(genericType).toByteBuffer(genericType);
        UUID actualValue = UUIDSerializer.get().fromByteBuffer(byteBuffer);
        assertThat(actualValue).isEqualTo(sourceValue);
    }

    @Test
    public void should_deserialize_uuid_type() throws Exception {
        String value = "c0178282-5c68-4fd3-9d9b-cae4f95fcd69";
        ByteBuffer byteBuffer = UUIDSerializer.get().toByteBuffer(UUID.fromString(value));
        GenericType genericType = GenericTypeSerializer.get(UUID_TYPE).fromByteBuffer(byteBuffer);
        assertThat(genericType.getValue()).isEqualTo(value);
        assertThat(genericType.getType()).isEqualTo(UUID_TYPE);
    }

    @Test
    public void should_serialize_composite_type() throws Exception {
        GenericType sourceValue = new GenericType(
                new String[] {"foo", "12345"},
                new GenericTypeEnum[] {UTF_8_TYPE, INTEGER_TYPE});
        ByteBuffer byteBuffer = GenericTypeSerializer.get(sourceValue).toByteBuffer(sourceValue);
        GenericType actualValue = GenericTypeSerializer.get(sourceValue).fromByteBuffer(byteBuffer);
        assertThat(actualValue).isEqualTo(sourceValue);
    }

    @Test
    public void should_deserialize_composite_type() throws Exception {
        Composite compositeValue = new Composite();
        compositeValue.addComponent(12345L,LongSerializer.get());
        compositeValue.addComponent("foo",StringSerializer.get());

        ByteBuffer byteBuffer = CompositeSerializer.get().toByteBuffer(compositeValue);
        GenericType genericType = GenericTypeSerializer.getComposite(LONG_TYPE, UTF_8_TYPE).fromByteBuffer(byteBuffer);
        assertThat(genericType.getCompositeValues()[0]).isEqualTo("12345");
        assertThat(genericType.getCompositeValues()[1]).isEqualTo("foo");
        assertThat(genericType.getType()).isEqualTo(COMPOSITE_TYPE);
    }
}
