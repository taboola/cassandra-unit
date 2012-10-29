package org.cassandraunit.assertion;

public class Difference {
    public final Object expected;
    public final Object found;

    public Difference(Object expected, Object found) {
        this.expected = expected;
        this.found = found;
    }

    @Override
    public String toString() {
        return "Difference{" +
                "expected=" + expected +
                ", found=" + found +
                '}';
    }
}
