package org.cassandraunit.utils;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.cassandraunit.dataset.ParseException;
import org.cassandraunit.type.GenericTypeEnum;
import org.junit.Test;

public class ComparatorTypeHelperTest {

	@Test
	public void shouldPassTheVerificationWithACompositeType() {
		try {
			ComparatorTypeHelper.verifyAndExtract("CompositeType(UTF8Type)");
		} catch (ParseException e) {
			fail();
		}
	}

	@Test
	public void shouldPassTheVerificationWithASimpleType() {
		try {
			ComparatorTypeHelper.verifyAndExtract("UTF8Type");
		} catch (ParseException e) {
			fail();
		}
	}

	@Test(expected = ParseException.class)
	public void shouldGetAnExceptionBecauseOfUnknownSimpleType() throws Exception {
		ComparatorTypeHelper.verifyAndExtract("UNKOWN");
	}

	@Test(expected = ParseException.class)
	public void shouldGetAnExceptionBecauseOfUnknownCompositeType() throws Exception {
		ComparatorTypeHelper.verifyAndExtract("CompositeType(UNKNOWN)");
	}

	@Test(expected = ParseException.class)
	public void shouldGetAnExceptionBecauseOfUnknownCompositeTypeAndGoodType() throws Exception {
		ComparatorTypeHelper.verifyAndExtract("CompositeType(UTF8Type,UNKNOWN)");
	}

	@Test(expected = ParseException.class)
	public void shouldGetAnExceptionBecauseOfCompositeTypeWithoutStartingWithAParenthesis() throws Exception {
		ComparatorTypeHelper.verifyAndExtract("CompositeTypeUTF8Type,LongType)");
	}

	@Test(expected = ParseException.class)
	public void shouldGetAnExceptionBecauseOfCompositeTypeWithoutEndingWithAParenthesis() throws Exception {
		ComparatorTypeHelper.verifyAndExtract("CompositeTypeUTF8Type,LongType");
	}

	@Test
	public void shouldExtractGenericTypesFromTypeAlias() throws Exception {
		GenericTypeEnum[] genericTypesEnum = ComparatorTypeHelper
				.extractGenericTypesFromTypeAlias("(LongType,UTF8Type,IntegerType)");
		assertThat(genericTypesEnum, is(new GenericTypeEnum[] { GenericTypeEnum.LONG_TYPE, GenericTypeEnum.UTF_8_TYPE,
				GenericTypeEnum.INTEGER_TYPE }));
	}
}
