package org.cassandraunit.utils;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

import org.cassandraunit.dataset.ParseException;
import org.junit.Test;

public class ComparatorTypeHelperTest {

	@Test
	public void shouldPassTheVerificationWithACompositeType() {
		try {
			ComparatorTypeHelper.verify("CompositeType(UTF8Type)");
		} catch (ParseException e) {
			fail();
		}
	}

	@Test
	public void shouldPassTheVerificationWithASimpleType() {
		try {
			ComparatorTypeHelper.verify("UTF8Type");
		} catch (ParseException e) {
			fail();
		}
	}

	@Test(expected = ParseException.class)
	public void shouldGetAnExceptionBecauseOfUnknownSimpleType() throws Exception {
		ComparatorTypeHelper.verify("UNKOWN");
	}

	@Test(expected = ParseException.class)
	public void shouldGetAnExceptionBecauseOfUnknownCompositeType() throws Exception {
		ComparatorTypeHelper.verify("CompositeType(UNKNOWN)");
	}

	@Test(expected = ParseException.class)
	public void shouldGetAnExceptionBecauseOfUnknownCompositeTypeAndGoodType() throws Exception {
		ComparatorTypeHelper.verify("CompositeType(UTF8Type,UNKNOWN)");
	}

	@Test(expected = ParseException.class)
	public void shouldGetAnExceptionBecauseOfCompositeTypeWithoutStartingWithAParenthesis() throws Exception {
		ComparatorTypeHelper.verify("CompositeTypeUTF8Type,LongType)");
	}

	@Test(expected = ParseException.class)
	public void shouldGetAnExceptionBecauseOfCompositeTypeWithoutEndingWithAParenthesis() throws Exception {
		ComparatorTypeHelper.verify("CompositeTypeUTF8Type,LongType");
	}

}
