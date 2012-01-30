package org.cassandraunit.utils;

import org.apache.commons.lang.StringUtils;
import org.cassandraunit.dataset.ParseException;
import org.cassandraunit.dataset.commons.ParsedDataType;

public class ComparatorTypeHelper {

	public static void verify(String comparatorType) {

		final String COMPOSITE_TYPE = "CompositeType";
		if (StringUtils.startsWith(comparatorType, COMPOSITE_TYPE)) {
			boolean error = false;
			/* CompositeType is defined */
			String aliasType = StringUtils.removeStart(comparatorType, COMPOSITE_TYPE);
			if (notStardOrNotEndWithParenthesis(aliasType)) {
				error = true;
			} else {
				String aliasTypeWithoutParenthesis = StringUtils
						.removeStart(StringUtils.removeEnd(aliasType, ")"), "(");
				String[] types = StringUtils.split(aliasTypeWithoutParenthesis, ",");
				if (typesNotNullAndNotEmpty(types)) {
					error = true;
				} else {
					/* has to be a known type */
					try {
						for (int i = 0; i < types.length; i++) {
							ParsedDataType.valueOf(types[i]);
						}
					} catch (IllegalArgumentException e) {
						error = true;
					}
				}
			}

			if (error) {
				throw new ParseException("CompositeType has to be like that : CompositeType(<type>,...,<type>)");
			}
		} else {
			/* standard Type */
			try {
				ParsedDataType.valueOf(comparatorType);
			} catch (IllegalArgumentException e) {
				throw new ParseException("ComparatorType value is not allowed");
			}
		}

	}

	private static boolean typesNotNullAndNotEmpty(String[] types) {
		return (types == null) || (types.length == 0);
	}

	private static boolean notStardOrNotEndWithParenthesis(String aliasType) {
		return !StringUtils.startsWith(aliasType, "(") || !StringUtils.endsWith(aliasType, ")");
	}

}
