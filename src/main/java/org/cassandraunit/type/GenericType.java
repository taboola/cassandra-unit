package org.cassandraunit.type;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class GenericType {
	private String value;
	private GenericTypeEnum type;

	public GenericType(String value, GenericTypeEnum type) {
		super();
		this.value = value;
		this.type = type;
	}

	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return value;
	}

	public GenericTypeEnum getType() {
		return type;
	}

}
