package org.cassandraunit.serializer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;

import java.nio.ByteBuffer;

public class GenericTypeDoubleSerializer extends AbstractSerializer<GenericType> {
    @Override
    public ByteBuffer toByteBuffer(GenericType obj) {
        return DoubleSerializer.get().toByteBuffer(Double.parseDouble(obj.getValue()));
    }

    @Override
    public GenericType fromByteBuffer(ByteBuffer byteBuffer) {
        return new GenericType(DoubleSerializer.get().fromByteBuffer(byteBuffer).toString(), GenericTypeEnum.DOUBLE_TYPE);
    }
}
