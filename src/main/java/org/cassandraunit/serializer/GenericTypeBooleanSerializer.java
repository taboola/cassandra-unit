package org.cassandraunit.serializer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;

import java.nio.ByteBuffer;

public class GenericTypeBooleanSerializer extends AbstractSerializer<GenericType> {
    @Override
    public ByteBuffer toByteBuffer(GenericType obj) {
        return BooleanSerializer.get().toByteBuffer(Boolean.parseBoolean(obj.getValue()));
    }

    @Override
    public GenericType fromByteBuffer(ByteBuffer byteBuffer) {
        Boolean booleanValue = BooleanSerializer.get().fromByteBuffer(byteBuffer);
        return new GenericType(booleanValue.toString(), GenericTypeEnum.BOOLEAN_TYPE);
    }
}
