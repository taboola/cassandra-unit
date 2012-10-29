package org.cassandraunit.serializer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import org.cassandraunit.type.GenericType;

import java.nio.ByteBuffer;

import static org.cassandraunit.type.GenericTypeEnum.INTEGER_TYPE;

public class GenericTypeIntegerSerializer extends AbstractSerializer<GenericType> {
    @Override
    public ByteBuffer toByteBuffer(GenericType obj) {
        return IntegerSerializer.get().toByteBuffer(Integer.valueOf(obj.getValue()));
    }

    @Override
    public GenericType fromByteBuffer(ByteBuffer byteBuffer) {
        Integer value = IntegerSerializer.get().fromByteBuffer(byteBuffer);
        return new GenericType(value.toString(), INTEGER_TYPE);
    }
}
