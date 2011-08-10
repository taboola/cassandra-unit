package org.cassandraunit.model;

public class KeyspaceModel {

	private String name;
	private int replicationFactor = 1;
	private String stategy = "org.apache.cassandra.locator.SimpleStrategy";

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getReplicationFactor() {
		return replicationFactor;
	}

	public void setReplicationFactor(int replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	public String getStategy() {
		return stategy;
	}

	public void setStategy(String stategy) {
		this.stategy = stategy;
	}

}
