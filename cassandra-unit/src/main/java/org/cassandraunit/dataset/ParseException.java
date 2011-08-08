package org.cassandraunit.dataset;

public class ParseException extends RuntimeException {

	public ParseException(Throwable e) {
		super(e);
	}

	public ParseException(String message) {
		super(message);
	}

}
