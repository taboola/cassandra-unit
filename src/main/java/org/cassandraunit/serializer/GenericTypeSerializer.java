package org.cassandraunit.serializer;

import java.nio.ByteBuffer;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.cassandraunit.exception.CassandraUnitException;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class GenericTypeSerializer extends AbstractSerializer<GenericType> {

	private static final GenericTypeSerializer instance = new GenericTypeSerializer();

	public static GenericTypeSerializer get() {
		return instance;
	}

	@Override
	public GenericType fromByteBuffer(ByteBuffer byteBuffer) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ByteBuffer toByteBuffer(GenericType genericType) {
		ByteBuffer byteBuffer = null;

		GenericTypeEnum currentType = genericType.getType();

		if (currentType == null) {
			currentType = GenericTypeEnum.BYTES_TYPE;
		}

		if (currentType == null) {

		} else {

			switch (genericType.getType()) {

			case BYTES_TYPE:
				byte[] hexDecodedBytes;
				try {
					hexDecodedBytes = Hex.decodeHex(genericType.getValue().toCharArray());
					byteBuffer = ByteBufferSerializer.get().fromBytes(hexDecodedBytes);
				} catch (DecoderException e) {
					throw new CassandraUnitException("cannot parse \"" + genericType.getValue() + "\" as hex bytes", e);
				}
				break;
			case INTEGER_TYPE:
				byteBuffer = IntegerSerializer.get().toByteBuffer(Integer.parseInt(genericType.getValue()));
				break;
			case LEXICAL_UUID_TYPE:
				byteBuffer = UUIDSerializer.get().toByteBuffer(UUID.fromString(genericType.getValue()));
				break;
			case LONG_TYPE:
				byteBuffer = LongSerializer.get().toByteBuffer(Long.parseLong(genericType.getValue()));
				break;
			case TIME_UUID_TYPE:
				byteBuffer = UUIDSerializer.get().toByteBuffer(UUID.fromString(genericType.getValue()));
				break;
			case UTF_8_TYPE:
				byteBuffer = StringSerializer.get().toByteBuffer(genericType.getValue());
				break;
			case UUID_TYPE:
				byteBuffer = UUIDSerializer.get().toByteBuffer(UUID.fromString(genericType.getValue()));
				break;
			case COUNTER_TYPE:
				byteBuffer = LongSerializer.get().toByteBuffer(Long.parseLong(genericType.getValue()));
				break;
			case COMPOSITE_TYPE:
				byteBuffer = new CompositeSerializer().toByteBuffer(createComposite(genericType));
				break;
			default:
				byteBuffer = BytesArraySerializer.get().toByteBuffer(genericType.getValue().getBytes());
				break;
			}
		}
		return byteBuffer;
	}

	private Composite createComposite(GenericType genericType) {
		if (!GenericTypeEnum.COMPOSITE_TYPE.equals(genericType.getType())) {
			throw new IllegalArgumentException("the generricType must be a CompositeType");
		}

		Composite composite = new Composite();
		for (int i = 0; i < genericType.getCompositeValues().length; i++) {
			composite.addComponent(
					new GenericType(genericType.getCompositeValues()[i],
							genericType.getTypesBelongingCompositeType()[i]), GenericTypeSerializer.get());
		}
		return composite;

	}
}
