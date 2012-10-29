package org.cassandraunit.serializer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.hector.api.Serializer;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import static com.google.common.collect.Maps.newHashMap;
import static java.util.Arrays.asList;
import static org.cassandraunit.type.GenericTypeEnum.*;

/**
 * @author Jeremy Sevellec
 */
public class GenericTypeSerializer extends AbstractSerializer<GenericType> {

    public static final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmmss");
    private static final HashMap<GenericTypeEnum,Serializer<GenericType>> serializersByType;

    private static final GenericTypeSerializer instance = new GenericTypeSerializer();

    static {
        serializersByType = newHashMap();
        serializersByType.put(ASCII_TYPE, new GenericTypeAsciiSerializer());
        serializersByType.put(BOOLEAN_TYPE, new GenericTypeBooleanSerializer());
        serializersByType.put(BYTES_TYPE, new GenericTypeBytesSerializer());
        serializersByType.put(COUNTER_TYPE, new GenericTypeCounterSerializer());
        serializersByType.put(DATE_TYPE, new GenericTypeDateSerializer());
        serializersByType.put(DOUBLE_TYPE, new GenericTypeDoubleSerializer());
        serializersByType.put(FLOAT_TYPE, new GenericTypeFloatSerializer());
        serializersByType.put(INTEGER_TYPE, new GenericTypeIntegerSerializer());
        serializersByType.put(LEXICAL_UUID_TYPE, new GenericTypeLexicalUuidSerializer());
        serializersByType.put(LONG_TYPE, new GenericTypeLongSerializer());
        serializersByType.put(TIME_UUID_TYPE, new GenericTypeTimeUuidSerializer());
        serializersByType.put(UTF_8_TYPE, new GenericTypeUtf8Serializer());
        serializersByType.put(UUID_TYPE, new GenericTypeUuidSerializer());
    }

    public static Serializer<GenericType> get(GenericTypeEnum type) {
        return serializersByType.get(type);
    }

    public static Serializer<GenericType> getComposite(GenericTypeEnum ... types) {
        return new GenericTypeCompositeTypeSerializer(asList(types));
    }

    public static Serializer<GenericType> get(GenericType genericType) {
        if (genericType.getType() == COMPOSITE_TYPE) {
            return new GenericTypeCompositeTypeSerializer(asList(genericType.getTypesBelongingCompositeType()));
        } else {
            GenericTypeEnum type = genericType.getType() != null ? genericType.getType() : BYTES_TYPE;
            return get(type);
        }
    }

    public static Serializer<GenericType> get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(GenericType obj) {
       return get(obj).toByteBuffer(obj);
    }

    @Override
    public GenericType fromByteBuffer(ByteBuffer byteBuffer) {
        throw new UnsupportedOperationException();
    }
}
