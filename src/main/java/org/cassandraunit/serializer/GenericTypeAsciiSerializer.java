package org.cassandraunit.serializer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;

import java.nio.ByteBuffer;

public class GenericTypeAsciiSerializer extends AbstractSerializer<GenericType> {
    @Override
    public ByteBuffer toByteBuffer(GenericType obj) {
        return StringSerializer.get().toByteBuffer(obj.getValue());
    }

    @Override
    public GenericType fromByteBuffer(ByteBuffer byteBuffer) {
        return new GenericType(StringSerializer.get().fromByteBuffer(byteBuffer), GenericTypeEnum.ASCII_TYPE);
    }
}
