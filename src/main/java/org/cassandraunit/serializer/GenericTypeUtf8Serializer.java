package org.cassandraunit.serializer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;

import java.nio.ByteBuffer;

public class GenericTypeUtf8Serializer extends AbstractSerializer<GenericType> {
    @Override
    public ByteBuffer toByteBuffer(GenericType obj) {
        return StringSerializer.get().toByteBuffer(obj.getValue());
    }

    @Override
    public GenericType fromByteBuffer(ByteBuffer byteBuffer) {
        String value = StringSerializer.get().fromByteBuffer(byteBuffer);
        return new GenericType(value.toString(), GenericTypeEnum.UTF_8_TYPE);
    }
}
