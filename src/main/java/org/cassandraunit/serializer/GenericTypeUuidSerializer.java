package org.cassandraunit.serializer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import org.cassandraunit.type.GenericType;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.cassandraunit.type.GenericTypeEnum.UUID_TYPE;

public class GenericTypeUuidSerializer extends AbstractSerializer<GenericType> {
    @Override
    public ByteBuffer toByteBuffer(GenericType obj) {
        return UUIDSerializer.get().toByteBuffer(UUID.fromString(obj.getValue()));
    }

    @Override
    public GenericType fromByteBuffer(ByteBuffer byteBuffer) {
        UUID value = UUIDSerializer.get().fromByteBuffer(byteBuffer);
        return new GenericType(value.toString(), UUID_TYPE);
    }
}
