package org.cassandraunit.serializer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.cassandraunit.exception.CassandraUnitException;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;

import java.nio.ByteBuffer;

public class GenericTypeBytesSerializer extends AbstractSerializer<GenericType> {
    @Override
    public ByteBuffer toByteBuffer(GenericType obj) {
        byte[] hexDecodedBytes;
        String genericValue = obj.getValue();
        try {
            if (genericValue.isEmpty()) {
                return ByteBufferSerializer.get().fromBytes(new byte[0]);
            } else {
                hexDecodedBytes = Hex.decodeHex(genericValue.toCharArray());
                return ByteBufferSerializer.get().fromBytes(hexDecodedBytes);
            }
        } catch (DecoderException e) {
            throw new CassandraUnitException("cannot parse \"" + genericValue + "\" as hex bytes", e);
        }
    }

    @Override
    public GenericType fromByteBuffer(ByteBuffer byteBuffer) {
        byte[] bytes = BytesArraySerializer.get().fromByteBuffer(byteBuffer);
        String stringValue = new String(Hex.encodeHex(bytes));
        return new GenericType(stringValue, GenericTypeEnum.BYTES_TYPE);
    }
}
