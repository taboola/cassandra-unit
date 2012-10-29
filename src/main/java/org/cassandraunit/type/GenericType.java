package org.cassandraunit.type;

import java.util.Arrays;

/**
 * @author Jeremy Sevellec
 */
public class GenericType {
    private String value;
    private GenericTypeEnum type;

    private String[] compositeValues;
    private GenericTypeEnum[] typesBelongingCompositeType;

    public GenericType(String value, GenericTypeEnum type) {
        super();
        this.value = value;
        this.type = type;
    }

    /**
     * constructor to use with the compositeType
     *
     * @param compositeValues             the string array of values
     * @param typesBelongingCompositeType the type array belonging the compositeType
     */
    public GenericType(String[] compositeValues, GenericTypeEnum[] typesBelongingCompositeType) {
        super();
        if ((compositeValues == null) || (typesBelongingCompositeType == null)
                || (compositeValues.length != typesBelongingCompositeType.length)) {
            throw new IllegalArgumentException(
                    "the 2 arrays must not be empty and must have the same size to match the value with the type into the compositeType");
        }
        this.compositeValues = compositeValues;
        this.type = GenericTypeEnum.COMPOSITE_TYPE;
        this.typesBelongingCompositeType = typesBelongingCompositeType;

    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GenericType that = (GenericType) o;

        if (!Arrays.equals(compositeValues, that.compositeValues)) return false;
        if (type != that.type) return false;
        if (!Arrays.equals(typesBelongingCompositeType, that.typesBelongingCompositeType)) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + type.hashCode();
        result = 31 * result + (compositeValues != null ? Arrays.hashCode(compositeValues) : 0);
        result = 31 * result + (typesBelongingCompositeType != null ? Arrays.hashCode(typesBelongingCompositeType) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "GenericType{" +
                "type=" + type +
                ", value='" + value + '\'' +
                '}';
    }

    public GenericTypeEnum getType() {
        return type;
    }

    public GenericTypeEnum[] getTypesBelongingCompositeType() {
        return typesBelongingCompositeType;
    }

    public String[] getCompositeValues() {
        return compositeValues;
    }

}
