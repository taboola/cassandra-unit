package org.cassandraunit.serializer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.hector.api.beans.Composite;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Iterables.toArray;
import static org.cassandraunit.type.GenericTypeEnum.COMPOSITE_TYPE;

public class GenericTypeCompositeTypeSerializer extends AbstractSerializer<GenericType> {

    private final List<GenericTypeEnum> componentTypes;

    public GenericTypeCompositeTypeSerializer(List<GenericTypeEnum> componentTypes) {
        this.componentTypes = componentTypes;
    }

    @Override
    public ByteBuffer toByteBuffer(GenericType obj) {
        return CompositeSerializer.get().toByteBuffer(createComposite(obj));
    }

    private Composite createComposite(GenericType genericType) {
        if (!COMPOSITE_TYPE.equals(genericType.getType())) {
            throw new IllegalArgumentException("the generricType must be a CompositeType");
        }

        Composite composite = new Composite();
        for (int i = 0; i < genericType.getCompositeValues().length; i++) {
            GenericType componentGenericType = new GenericType(genericType.getCompositeValues()[i], genericType.getTypesBelongingCompositeType()[i]);
            composite.addComponent(
                    componentGenericType,
                    GenericTypeSerializer.get(componentGenericType));
        }
        return composite;
    }

    @Override
    public GenericType fromByteBuffer(ByteBuffer byteBuffer) {
        Composite value = CompositeSerializer.get().fromByteBuffer(byteBuffer);
        List<String> values = new ArrayList<String>();
        for(int i=0; i < value.size(); i++) {
            GenericTypeEnum componentType = componentTypes.get(i);
            ByteBuffer componentBuffer = (ByteBuffer) value.get(i);
            GenericType genericType = GenericTypeSerializer.get(componentType).fromByteBuffer(componentBuffer);
            values.add(genericType.getValue());
        }
        return new GenericType(toArray(values, String.class), toArray(componentTypes, GenericTypeEnum.class));
    }
}
