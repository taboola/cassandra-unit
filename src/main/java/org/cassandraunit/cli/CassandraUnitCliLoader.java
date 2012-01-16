package org.cassandraunit.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class CassandraUnitCliLoader {

	private static CommandLineParser commandLineParser = null;

	private static Options options = null;

	private static CommandLine commandLine = null;

	private static boolean usageBeenPrinted;

	static {

	}

	/**
	 * @param args
	 * @throws ParseException
	 */
	public static void main(String[] args) {
		clear();

		initOptions();

		commandLineParser = new PosixParser();
		try {
			commandLine = commandLineParser.parse(options, args);
			if (commandLine.getOptions().length == 0) {
				printUsage();
			} else {
				if (containBadReplicationFactorArgumentValue()) {
					printUsage("Bad argument value for option r");
				}
			}

			System.out.println("Load complete");
		} catch (ParseException e) {
			printUsage(e.getMessage());
		}

	}

	private static boolean containBadReplicationFactorArgumentValue() {
		String replicationFactor = commandLine.getOptionValue("r");
		if (replicationFactor != null && !replicationFactor.trim().isEmpty()) {
			try {
				Integer.parseInt(replicationFactor);
				return false;
			} catch (NumberFormatException e) {
				return true;
			}
		}
		return false;

	}

	private static void printUsage(String message) {
		System.out.println(message);
		printUsage();

	}

	private static void initOptions() {
		options = new Options();
		options.addOption(OptionBuilder.withLongOpt("file").hasArg().withDescription("dataset to load").isRequired()
				.create("f"));
		options.addOption(OptionBuilder.withLongOpt("host").hasArg().withDescription("target host (required)")
				.isRequired().create("h"));
		options.addOption(OptionBuilder.withLongOpt("port").hasArg().withDescription("target port (required)")
				.isRequired().create("p"));
		options.addOption(OptionBuilder.withLongOpt("onlySchema").withDescription("only load schema (optional)")
				.create("o"));
		options.addOption(OptionBuilder.withLongOpt("replicationFactor").hasArg()
				.withDescription("override the replication factor set in the dataset (optional)").create("r"));
		options.addOption(OptionBuilder.withLongOpt("strategy").hasArg()
				.withDescription("override the strategy set in the dataset (optional)").create("s"));

	}

	private static void clear() {
		commandLine = null;
		commandLineParser = null;
		options = null;
		usageBeenPrinted = false;
	}

	private static void printUsage() {
		usageBeenPrinted = true;
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("load", options);
	}

	protected static CommandLine getCommandLine() {
		return commandLine;
	}

	protected static boolean isUsageBeenPrinted() {
		return usageBeenPrinted;
	}
}
