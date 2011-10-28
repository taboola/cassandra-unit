package org.cassandraunit.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class EmbeddedCassandraServerHelper {

	private static final String INTERNAL_CASSANDRA_KEYSPACE = "system";

	private static Logger log = LoggerFactory.getLogger(EmbeddedCassandraServerHelper.class);

	private static final String TMP = "target/embeddedCassandra";

	private static final String yamlFile = "/cassandra.yaml";

	private static EmbeddedCassandraService embeddedCassandraService = null;

	public EmbeddedCassandraServerHelper() {

	}

	/**
	 * Set embedded cassandra up and spawn it in a new thread.
	 * 
	 * @throws TTransportException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void startEmbeddedCassandra() throws TTransportException, IOException, InterruptedException,
			ConfigurationException {

		if (embeddedCassandraService == null) {
			log.debug("Starting cassandra...");
			log.debug("Initialization needed");
			/* new JVM, so init all an instance off Cassandra */
			/* delete tmp dir first */
			rmdir(TMP);

			/* make a tmp dir and copy cassandra.yaml and log4j.properties to it */
			copy("/log4j-embedded-cassandra.properties", TMP);
			copy(yamlFile, TMP);

			/* set system properties for cassandra */
			System.setProperty("cassandra.config", "file:" + TMP + yamlFile);
			System.setProperty("log4j.configuration", "file:" + TMP + "/log4j-embedded-cassandra.properties");
			System.setProperty("cassandra-foreground", "true");
			cleanupAndLeaveDirs();
			embeddedCassandraService = new EmbeddedCassandraService();
			embeddedCassandraService.start();
		} else {
			/* nothing to do */
		}

	}

	public static void cleanEmbeddedCassandra() {
		log.debug("Cleaning cassandra keyspaces");
		dropKeyspaces();
	}

	private static void dropKeyspaces() {
		Cluster cluster = HFactory.getOrCreateCluster("TestCluster", new CassandraHostConfigurator("localhost:9171"));
		/* get all keyspace */
		List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();

		/* drop all keyspace except internal cassandra keyspace */
		for (KeyspaceDefinition keyspaceDefinition : keyspaces) {
			String keyspaceName = keyspaceDefinition.getName();

			if (!INTERNAL_CASSANDRA_KEYSPACE.equals(keyspaceName)) {
				cluster.dropKeyspace(keyspaceName);
			}
		}
	}

	private static void rmdir(String dir) throws IOException {
		File dirFile = new File(dir);
		if (dirFile.exists()) {
			FileUtils.deleteRecursive(new File(dir));
		}
	}

	/**
	 * Copies a resource from within the jar to a directory.
	 * 
	 * @param resource
	 * @param directory
	 * @throws IOException
	 */
	private static void copy(String resource, String directory) throws IOException {
		mkdir(directory);
		InputStream is = EmbeddedCassandraServerHelper.class.getResourceAsStream(resource);
		String fileName = resource.substring(resource.lastIndexOf("/") + 1);
		File file = new File(directory + System.getProperty("file.separator") + fileName);
		OutputStream out = new FileOutputStream(file);
		byte buf[] = new byte[1024];
		int len;
		while ((len = is.read(buf)) > 0) {
			out.write(buf, 0, len);
		}
		out.close();
		is.close();
	}

	/**
	 * Creates a directory
	 * 
	 * @param dir
	 * @throws IOException
	 */
	private static void mkdir(String dir) throws IOException {
		FileUtils.createDirectory(dir);
	}

	private static void cleanupAndLeaveDirs() throws IOException {
		mkdirs();
		cleanup();
		mkdirs();
		CommitLog.instance.resetUnsafe(); // cleanup screws w/ CommitLog, this
											// brings it back to safe state
	}

	private static void cleanup() throws IOException {
		// clean up commitlog
		String[] directoryNames = { DatabaseDescriptor.getCommitLogLocation(), };
		for (String dirName : directoryNames) {
			File dir = new File(dirName);
			if (!dir.exists())
				throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
			FileUtils.deleteRecursive(dir);
		}

		// clean up data directory which are stored as data directory/table/data
		// files
		for (String dirName : DatabaseDescriptor.getAllDataFileLocations()) {
			File dir = new File(dirName);
			if (!dir.exists())
				throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
			FileUtils.deleteRecursive(dir);
		}
	}

	public static void mkdirs() {
		try {
			DatabaseDescriptor.createAllDirectories();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
