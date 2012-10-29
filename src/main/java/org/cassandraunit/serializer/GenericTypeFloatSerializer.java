package org.cassandraunit.serializer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.FloatSerializer;
import org.cassandraunit.type.GenericType;

import java.nio.ByteBuffer;

import static org.cassandraunit.type.GenericTypeEnum.FLOAT_TYPE;

public class GenericTypeFloatSerializer extends AbstractSerializer<GenericType> {
    @Override
    public ByteBuffer toByteBuffer(GenericType obj) {
        return FloatSerializer.get().toByteBuffer(Float.parseFloat(obj.getValue()));
    }

    @Override
    public GenericType fromByteBuffer(ByteBuffer byteBuffer) {
        Float value = FloatSerializer.get().fromByteBuffer(byteBuffer);
        return new GenericType(value.toString(), FLOAT_TYPE);
    }
}
