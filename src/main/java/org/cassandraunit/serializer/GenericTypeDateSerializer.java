package org.cassandraunit.serializer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import org.cassandraunit.exception.CassandraUnitException;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

public class GenericTypeDateSerializer extends AbstractSerializer<GenericType> {

    public static final DateFormat DATE_FORMAT = GenericTypeSerializer.dateFormat;

    @Override
    public ByteBuffer toByteBuffer(GenericType obj) {
        String genericValue = obj.getValue();
        try {
            return DateSerializer.get().toByteBuffer(DATE_FORMAT.parse(genericValue));
        } catch (ParseException e) {
            throw new CassandraUnitException("cannot parse \"" + genericValue + "\" as date", e);
        }
    }

    @Override
    public GenericType fromByteBuffer(ByteBuffer byteBuffer) {
        Date date = DateSerializer.get().fromByteBuffer(byteBuffer);
        return new GenericType(DATE_FORMAT.format(date), GenericTypeEnum.DATE_TYPE);
    }
}
