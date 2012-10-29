package org.cassandraunit.serializer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;

import java.nio.ByteBuffer;

public class GenericTypeCounterSerializer extends AbstractSerializer<GenericType> {
    @Override
    public ByteBuffer toByteBuffer(GenericType obj) {
        return LongSerializer.get().toByteBuffer(Long.parseLong(obj.getValue()));
    }

    @Override
    public GenericType fromByteBuffer(ByteBuffer byteBuffer) {
        Long value = LongSerializer.get().fromByteBuffer(byteBuffer);
        return new GenericType(value.toString(), GenericTypeEnum.COUNTER_TYPE);
    }
}
