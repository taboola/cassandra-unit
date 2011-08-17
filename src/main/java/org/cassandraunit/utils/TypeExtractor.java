package org.cassandraunit.utils;

import me.prettyprint.hector.api.ddl.ComparatorType;

import org.apache.commons.lang.StringUtils;
import org.cassandraunit.type.GenericType;
import org.cassandraunit.type.GenericTypeEnum;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class TypeExtractor {

	private static String endTypeFunction = ")";
	private static String startTypeFunction = "(";
	private static String[] availableTypeFunctionArray = new String[] { "bytes(", "integer(", "lexicaluuid(", "long(",
			"utf8(", "timeuuid(", "uuid(" };

	public static GenericType extract(String valueToExtract, ComparatorType defaultValueType) {
		String extractedValue = null;
		GenericType genericType = null;
		if (StringUtils.startsWithAny(valueToExtract, availableTypeFunctionArray)
				&& StringUtils.endsWith(valueToExtract, endTypeFunction)) {
			/* there is a type function defined */
			String typeFunction = StringUtils.substringBefore(valueToExtract, startTypeFunction);
			String tmp = StringUtils.substringAfter(valueToExtract, typeFunction + startTypeFunction);
			extractedValue = StringUtils.substringBefore(tmp, endTypeFunction);
			genericType = new GenericType(extractedValue, GenericTypeEnum.fromValue(typeFunction + "type"));
		} else {
			/* there is no type function defined */
			extractedValue = valueToExtract;
			if (defaultValueType == null) {
				genericType = new GenericType(extractedValue, GenericTypeEnum.BYTES_TYPE);
			} else {
				genericType = new GenericType(extractedValue, GenericTypeEnum.fromValue(defaultValueType.getTypeName()));
			}
		}
		return genericType;
	}
}
