package org.ai.wu.ac.at.tdbQuery.Parallel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.util.FileManager;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class TB_Archive {

	private static int TOTALVERSIONS = 58;

	private static String outputDIR = "results";

	public static void main(String[] args) throws FileNotFoundException {

		String dirTDB = null;
		String queryFile = null;
		String queryFileDynamic = null;
		Boolean bulkQueries = false;
		String outputResults = "resultsApp.txt";
		String queryCategory = "all"; // query everything;
		String outputTime = "timeApp.txt";
		Options options = new Options();
		int versionQuery = 0;
		int postversionQuery = 0;
		int jump = 0;
		String rol = "subject"; // for bulk queries
		try {

			Option inputDirOpt = new Option("d", "dir", true, "TDB DIR to load");
			inputDirOpt.setRequired(true);
			options.addOption(inputDirOpt);

			Option queryOpt = new Option("q", "query", true,
					"SPARQL query to process");
			queryOpt.setRequired(false);
			options.addOption(queryOpt);

			Option filequeryOpt = new Option("f", "file", true,
					"static queries to process");
			filequeryOpt.setRequired(false);
			options.addOption(filequeryOpt);

			Option fileDynqueryOpt = new Option("F",
					"file with dynamic queries", true,
					"dynamic queries to process");
			fileDynqueryOpt.setRequired(false);
			options.addOption(fileDynqueryOpt);

			Option queryCatOpt = new Option("c", "category", true,
					"Query category: mat | diff | ver | all");
			queryCatOpt.setRequired(false);
			options.addOption(queryCatOpt);

			Option versionOpt = new Option("v", "version", true,
					"Version, used in the Query (e.g. in materialize)");
			versionOpt.setRequired(false);
			options.addOption(versionOpt);
			Option postversionOpt = new Option("p", "postversion", true,
					"Version 2, used in the Query (e.g. in diff)");
			postversionOpt.setRequired(false);
			options.addOption(postversionOpt);

			Option jumpCatOpt = new Option("j", "jump", true,
					"Jump step for the diff: e.g. 5 (0-5,0-10..)");
			jumpCatOpt.setRequired(false);
			options.addOption(jumpCatOpt);

			Option outputOpt = new Option("o", "OutputRewriting", true,
					"SPARQL query to process");
			outputOpt.setRequired(false);
			options.addOption(outputOpt);

			Option outputDirOpt = new Option("O", "OutputDir", true,
					"Output directory");
			outputDirOpt.setRequired(false);
			options.addOption(outputDirOpt);

			Option rolOpt = new Option("r", "rol", true,
					"Rol of the Resource in the query: subject (s) | predicate (p) | object (o)");
			rolOpt.setRequired(false);
			options.addOption(rolOpt);

			Option timeOpt = new Option("t", "timeOutput", true,
					"file to write the output");
			timeOpt.setRequired(false);
			options.addOption(timeOpt);

			Option helpOpt = new Option("h", "help", false, "Shows help");
			helpOpt.setRequired(false);
			options.addOption(helpOpt);

			// Parse input arguments
			CommandLineParser cliParser = new BasicParser();
			CommandLine cmdLine = cliParser.parse(options, args);

			// Read arguments

			if (cmdLine.hasOption("d")) {
				dirTDB = cmdLine.getOptionValue("d");
			}
			if (cmdLine.hasOption("c")) {
				queryCategory = cmdLine.getOptionValue("c");
			}
			if (cmdLine.hasOption("v")) {
				versionQuery = Integer.parseInt(cmdLine.getOptionValue("v"));
			}
			if (cmdLine.hasOption("p")) {
				postversionQuery = Integer
						.parseInt(cmdLine.getOptionValue("p"));
			}
			if (cmdLine.hasOption("o")) {
				outputResults = cmdLine.getOptionValue("o");
			}
			if (cmdLine.hasOption("O")) {
				outputDIR = cmdLine.getOptionValue("O");
			}
			if (cmdLine.hasOption("t")) {
				outputTime = cmdLine.getOptionValue("t");
			}
			if (cmdLine.hasOption("q")) {
				queryFile = cmdLine.getOptionValue("q");
			}

			if (cmdLine.hasOption("f")) {
				queryFile = cmdLine.getOptionValue("f");
				bulkQueries = true;
			}
			if (cmdLine.hasOption("F")) {
				queryFileDynamic = cmdLine.getOptionValue("F");
				bulkQueries = true;
			}

			if (cmdLine.hasOption("h") || cmdLine.hasOption("help")) {
				HelpFormatter format = new HelpFormatter();
				format.printHelp("App", options);
			}
			if (cmdLine.hasOption("r")) {
				rol = cmdLine.getOptionValue("r");
			}
			if (cmdLine.hasOption("j")) {
				jump = Integer.parseInt(cmdLine.getOptionValue("j"));
			}

		} catch (ParseException e) {
			HelpFormatter format = new HelpFormatter();
			format.printHelp("App", options);
			Logger.getLogger(TB_Archive.class.getName()).log(Level.SEVERE,
					null, e);
		} catch (Exception e) {
			e.printStackTrace();
		}

		OutputStream os = new FileOutputStream(outputResults);
		OutputStream os_time = new FileOutputStream(outputTime);
		PrintStream printStream = new PrintStream(os);
		PrintStream printStreamTime = new PrintStream(os_time);

		// Initilize Jena
		FileManager fm = FileManager.get();
		fm.addLocatorClassLoader(TB_Archive.class.getClassLoader());

		Dataset dataset = TDBFactory.createDataset(dirTDB);

		try {
			System.out.println("Query is " + queryCategory);
			if (queryCategory.equalsIgnoreCase("all")) {
				String queryString = readFile(queryFile, StandardCharsets.UTF_8);
				System.out.println(queryString);
				Query query = QueryFactory.create(queryString);
				long startTime = System.currentTimeMillis();
				QueryExecution qexec = QueryExecutionFactory.create(query,
						dataset);
				try {
					ResultSet results = qexec.execSelect();
					while (results.hasNext()) {
						QuerySolution soln = results.nextSolution();
						RDFNode solElement = soln.get("element");

						// Resource solElement = soln.getResource("element");
						Literal count = soln.getLiteral("countElement");
						printStream.println(solElement);
						System.out.println("Result:" + solElement);
						System.out.println("count:" + count);
					}
				} finally {
					long endTime = System.currentTimeMillis();
					System.out.println("Time:" + (endTime - startTime));
					printStreamTime.println(queryFile + ","
							+ (endTime - startTime));
					qexec.close();
				}
			} else if (queryCategory.equalsIgnoreCase("mat")) {

				/*
				 * ONE QUERY
				 */
				if (!bulkQueries) {
					String queryString = readFile(queryFile,
							StandardCharsets.UTF_8);
					System.out.println("Query is '" + queryCategory
							+ "' at version " + versionQuery);
					System.out.println(queryString);

					matQuery(queryFile, versionQuery, printStream,
							printStreamTime, dataset, queryString);
				}
				/*
				 * FILE CONTAINS SEVERAL QUERIES
				 */
				else {
					if (queryFile != null)
						bulkMatQuerying(queryFile, rol, dataset);
					else
						bulkAllMatQuerying(queryFileDynamic, rol, dataset);
				}
			} else if (queryCategory.equalsIgnoreCase("diff")) {

				if (!bulkQueries) {
					System.out.println("Diff between version " + versionQuery
							+ " and version " + postversionQuery);
					String queryString = readFile(queryFile,
							StandardCharsets.UTF_8);

					System.out.println(queryString);
					diffQuerying(queryFile, versionQuery, postversionQuery,
							printStream, printStreamTime, dataset, queryString);
				}
				/*
				 * FILE CONTAINS SEVERAL QUERIES
				 */
				else {
					System.out.println("Diff of all versions ");
					if (jump > 0)
						System.out.println("     with jump " + jump);
					bulkAlldiffQuerying(queryFileDynamic, rol, dataset, jump);
				}
			}

			else if (queryCategory.equalsIgnoreCase("ver")) {
				if (!bulkQueries) {
					String queryString = readFile(queryFile,
							StandardCharsets.UTF_8);
					System.out.println(queryString);
					verQuery(queryFile, printStream, printStreamTime, dataset,
							queryString);
				} else {
					bulkAllVerQuerying(queryFileDynamic, rol, dataset);
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		dataset.end();
		printStream.close();
		printStreamTime.close();
	}

	/**
	 * @param queryFile
	 * @param versionQuery
	 * @param postversionQuery
	 * @param printStream
	 * @param printStreamTime
	 * @param dataset
	 * @param queryString
	 */
	private static void diffQuerying(String queryFile, int versionQuery,
			int postversionQuery, PrintStream printStream,
			PrintStream printStreamTime, Dataset dataset, String queryString) {
		Query query = QueryFactory.create(queryString);
		long startTime = System.currentTimeMillis();
		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
		try {
			ResultSet results = qexec.execSelect();

			Boolean higherVersion1 = false;
			Boolean higherVersion2 = false;
			HashSet<String> finalResultsAdd = new HashSet<String>();
			HashSet<String> finalResultsDel = new HashSet<String>();

			Iterator<QuerySolution> sortResults = orderedResultSet(results,
					"graph");

			QuerySolution soln = null;
			while (sortResults.hasNext() && (!higherVersion1 | !higherVersion2)) {
				soln = sortResults.next();
				// assume we have a graph variable as a response
				String graphResponse = soln.getResource("graph").toString();
				String versionSuffix = graphResponse.split("version")[1];
				int versionFull = Integer.parseInt(versionSuffix);

				int version = versionFull / 2;

				if (version > versionQuery) {
					higherVersion1 = true;
				}

				if (higherVersion1) {
					System.out.println("going between both versions");
					System.out.println("--version:" + version);
					Boolean isAdd = false; // true if is Deleted
					if (versionFull % 2 == 0) {
						isAdd = true;
					}

					if (version > postversionQuery) {
						higherVersion2 = true;
					} else {

						// assume we have element1 and element2
						// variables as
						// a response
						String rowResult = soln.get("element1").toString()
								+ " " + soln.get("element2");

						System.out.println("****** RowResult: " + rowResult);

						if (isAdd) {
							// check if it was already as a delete
							// result and, if so, delete this
							if (!finalResultsDel.remove(rowResult))
								finalResultsAdd.add(rowResult);
						} else {
							// check if it was already as an added
							// result and, if so, delete this
							if (!finalResultsAdd.remove(rowResult))
								finalResultsDel.add(rowResult);
						}

					}
				}
			}
			Iterator<String> it = finalResultsAdd.iterator();
			String res;
			while (it.hasNext()) {
				res = it.next();
				// element is the response
				printStream.println(">" + res);
				// System.out.println("count:" + count);
			}
			it = finalResultsDel.iterator();
			while (it.hasNext()) {
				res = it.next();
				// element is the response
				printStream.println("<" + res);
				// System.out.println("count:" + count);
			}

		} finally {
			long endTime = System.currentTimeMillis();
			System.out.println("Time:" + (endTime - startTime));
			printStreamTime.println(queryFile + "," + (endTime - startTime));
			qexec.close();
		}
	}

	/**
	 * @param queryFile
	 * @param rol
	 * @param dataset
	 * @throws IOException
	 * @throws NumberFormatException
	 */
	private static void bulkAlldiffQuerying(String queryFile, String rol,
			Dataset dataset, int jump) throws NumberFormatException,
			IOException {

		File inputFile = new File(queryFile);
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = "";

		TreeMap<Integer, DescriptiveStatistics> vStats = new TreeMap<Integer, DescriptiveStatistics>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			vStats.put(i, new DescriptiveStatistics());
		}

		DescriptiveStatistics total = new DescriptiveStatistics();

		while ((line = br.readLine()) != null) {

			String[] parts = line.split(" ");
			String element = parts[0];

			/*
			 * warmup the system
			 */
			warmup(dataset);

			int start = 0;
			int end = TOTALVERSIONS - 1;
			if (jump > 0) {
				end = ((TOTALVERSIONS - 1) / jump) + 1; // +1 to do one loop at
														// least
			}
			for (int index = start; index < end; index++) {
				int versionQuery = index;
				int postversionQuery = versionQuery + 1;
				if (jump > 0) {
					postversionQuery = Math.min((index + 1) * jump,
							TOTALVERSIONS - 1);
					versionQuery = 0;
				}
				System.out.println("versionQuery:" + versionQuery
						+ " ; postQuery:" + postversionQuery);

				String queryString = createQuery(rol, element);
				System.out.println("queryString:" + queryString);
				Query query = QueryFactory.create(queryString);
				long startTime = System.currentTimeMillis();
				QueryExecution qexec = QueryExecutionFactory.create(query,
						dataset);

				ResultSet results = qexec.execSelect();

				Boolean higherVersion1 = false;
				Boolean higherVersion2 = false;
				HashSet<String> finalResultsAdd = new HashSet<String>();
				HashSet<String> finalResultsDel = new HashSet<String>();

				Iterator<QuerySolution> sortResults = orderedResultSet(results,
						"graph");

				QuerySolution soln = null;
				while (sortResults.hasNext()
						&& (!higherVersion1 | !higherVersion2)) {
					soln = sortResults.next();
					// assume we have a graph variable as a response
					String graphResponse = soln.getResource("graph").toString();
					String versionSuffix = graphResponse.split("version")[1];
					int versionFull = Integer.parseInt(versionSuffix);

					int version = versionFull / 2;

					if (version > versionQuery) {
						higherVersion1 = true;
					}

					if (higherVersion1) {
						// System.out.println("going between both versions");
						// System.out.println("--version:" + version);
						Boolean isAdd = false; // true if is Deleted
						if (versionFull % 2 == 0) {
							isAdd = true;
						}

						if (version > postversionQuery) {
							higherVersion2 = true;
						} else {

							// assume we have element1 and element2
							// variables as
							// a response
							String rowResult = soln.get("element1").toString()
									+ " " + soln.get("element2");

							// System.out.println("****** RowResult: " +
							// rowResult);

							if (isAdd) {
								// check if it was already as a delete
								// result and, if so, delete this
								if (!finalResultsDel.remove(rowResult))
									finalResultsAdd.add(rowResult);
							} else {
								// check if it was already as an added
								// result and, if so, delete this
								if (!finalResultsAdd.remove(rowResult))
									finalResultsDel.add(rowResult);
							}

						}
					}
				}
				Iterator<String> it = finalResultsAdd.iterator();
				String res;
				while (it.hasNext()) {
					res = it.next();
					// element is the response
					// printStream.println(">" + res);
					// System.out.println("count:" + count);
				}
				it = finalResultsDel.iterator();
				while (it.hasNext()) {
					res = it.next();
					// element is the response
					// printStream.println("<" + res);
					// System.out.println("count:" + count);
				}

				long endTime = System.currentTimeMillis();
				System.out.println("Time:" + (endTime - startTime));
				total.addValue((endTime - startTime));
				vStats.get(index).addValue((endTime - startTime));
				// printStreamTime.println(queryFile + "," + (endTime -
				// startTime));
				qexec.close();

			}
		}
		String outFile = outputDIR + "/res-dyndiff-" + inputFile.getName();
		if (jump > 0)
			outFile = outputDIR + "/res-dyndiff-jump" + jump + "-"
					+ inputFile.getName();
		PrintWriter pw = new PrintWriter(new File(outFile));
		pw.println("##bucket, min, mean, max, stddev, count");
		for (Entry<Integer, DescriptiveStatistics> ent : vStats.entrySet()) {
			pw.println(ent.getKey() + " " + ent.getValue().getMin() + " "
					+ ent.getValue().getMean() + " " + ent.getValue().getMax()
					+ " " + ent.getValue().getStandardDeviation() + " "
					+ ent.getValue().getN());
		}
		pw.println("tot," + total.getMin() + "," + total.getMean() + ","
				+ total.getMax() + "," + total.getStandardDeviation() + ","
				+ total.getN());
		pw.close();
	}

	/**
	 * @param queryFile
	 * @param versionQuery
	 * @param printStream
	 * @param printStreamTime
	 * @param dataset
	 * @param queryString
	 */
	private static void matQuery(String queryFile, int versionQuery,
			PrintStream printStream, PrintStream printStreamTime,
			Dataset dataset, String queryString) {
		Query query = QueryFactory.create(queryString);
		long startTime = System.currentTimeMillis();
		materializeQuery(dataset, versionQuery, query);

		long endTime = System.currentTimeMillis();
		System.out.println("Time:" + (endTime - startTime));
		printStreamTime.println(queryFile + "," + (endTime - startTime));
	}

	/**
	 * @param queryFile
	 * @param rol
	 * @param dataset
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static void bulkMatQuerying(String queryFile, String rol,
			Dataset dataset) throws FileNotFoundException, IOException {

		File inputFile = new File(queryFile);
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = "";
		TreeMap<Integer, DescriptiveStatistics> vStats = new TreeMap<Integer, DescriptiveStatistics>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			vStats.put(i, new DescriptiveStatistics());
		}

		while ((line = br.readLine()) != null) {
			String[] parts = line.split(",");
			int staticVersionQuery = Integer.parseInt(parts[0]);
			String element = parts[3];
			String queryString = createQuery(rol, element);

			System.out.println("Query at version " + staticVersionQuery);
			System.out.println(queryString);
			// then test the infile
			// System.out.println("Reading Query from file at version " +
			// staticVersionQuery);
			// queryString = readFile("testQ", StandardCharsets.UTF_8);
			// System.out.println(queryString);

			Query query = QueryFactory.create(queryString);
			long startTime = System.currentTimeMillis();

			// TOTALVERSIONS
			int numfinalResults = materializeQuery(dataset, staticVersionQuery,
					query);

			long endTime = System.currentTimeMillis();
			System.out.println("Time:" + (endTime - startTime));

			vStats.get(staticVersionQuery).addValue((endTime - startTime));

		}
		br.close();

		PrintWriter pw = new PrintWriter(new File(outputDIR + "/res-statmat-"
				+ inputFile.getName()));
		pw.println("##ver, min, mean, max, stddev, count");
		for (Entry<Integer, DescriptiveStatistics> ent : vStats.entrySet()) {
			pw.println(ent.getKey() + " " + ent.getValue().getMin() + " "
					+ ent.getValue().getMean() + " " + ent.getValue().getMax()
					+ " " + ent.getValue().getStandardDeviation() + " "
					+ ent.getValue().getN());
		}
		pw.close();
	}

	private static String createQuery(String rol, String element) {
		String queryString = "SELECT ?element1 ?element2 ?graph WHERE { "
				+ "GRAPH ?graph{";
		if (rol.equalsIgnoreCase("subject") || rol.equalsIgnoreCase("s")
				|| rol.equalsIgnoreCase("subjects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + element + " ?element1 ?element2 .";
		} else if (rol.equalsIgnoreCase("predicate")
				|| rol.equalsIgnoreCase("p")
				|| rol.equalsIgnoreCase("predicates")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 " + element + " ?element2 .";
		} else if (rol.equalsIgnoreCase("object") || rol.equalsIgnoreCase("o")
				|| rol.equalsIgnoreCase("objects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 ?element2 " + element + " .";
		}
		queryString = queryString + "}" + "}";

		return queryString;
	}

	/**
	 * @param queryFile
	 * @param rol
	 * @param dataset
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static void bulkAllMatQuerying(String queryFile, String rol,
			Dataset dataset) throws FileNotFoundException, IOException {
		int[] timeVersion = new int[59];
		int[] resultVersion = new int[59];
		int[] numQueriesVersion = new int[59];
		File inputFile = new File(queryFile);
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = "";

		TreeMap<Integer, DescriptiveStatistics> vStats = new TreeMap<Integer, DescriptiveStatistics>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			vStats.put(i, new DescriptiveStatistics());
		}

		while ((line = br.readLine()) != null) {

			String[] parts = line.split(" ");

			String element = parts[0];

			String queryString = createQuery(rol, element);

			System.out.println(queryString);
			// then test the infile
			// System.out.println("Reading Query from file at version " +
			// staticVersionQuery);
			// queryString = readFile("testQ", StandardCharsets.UTF_8);
			// System.out.println(queryString);

			Query query = QueryFactory.create(queryString);

			/*
			 * warmup the system
			 */
			warmup(dataset);

			for (int i = 0; i < TOTALVERSIONS; i++) {
				// System.out.println("Query at version " + i);
				long startTime = System.currentTimeMillis();
				int numfinalResults = materializeQuery(dataset, i, query);

				long endTime = System.currentTimeMillis();
				// System.out.println("Time:" + (endTime - startTime));

				vStats.get(i).addValue((endTime - startTime));

				timeVersion[i] += (endTime - startTime);
				resultVersion[i] += numfinalResults;
				numQueriesVersion[i] += 1;
			}
		}
		br.close();

		PrintWriter pw = new PrintWriter(new File(outputDIR + "/res-dynmat-"
				+ inputFile.getName()));
		pw.println("##ver, min, mean, max, stddev, count");
		for (Entry<Integer, DescriptiveStatistics> ent : vStats.entrySet()) {
			pw.println(ent.getKey() + " " + ent.getValue().getMin() + " "
					+ ent.getValue().getMean() + " " + ent.getValue().getMax()
					+ " " + ent.getValue().getStandardDeviation() + " "
					+ ent.getValue().getN());
		}
		pw.close();
	}

	/**
	 * @param dataset
	 * @param staticVersionQuery
	 * @param query
	 * @return
	 */
	private static int materializeQuery(Dataset dataset,
			int staticVersionQuery, Query query) {
		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
		int numfinalResults = 0;

		ResultSet results = qexec.execSelect();

		Boolean higherVersion = false;
		HashSet<String> finalResults = new HashSet<String>();

		Iterator<QuerySolution> sortResults = orderedResultSet(results, "graph");
		int numRows = 0;
		while (sortResults.hasNext() && !higherVersion) {
			numRows++;
			QuerySolution soln = sortResults.next();
			// assume we have a graph variable as a response
			String graphResponse = soln.getResource("graph").toString();
			// System.out.println("--graphResponse:" + graphResponse);
			String versionSuffix = graphResponse.split("version")[1];
			// System.out.println("--versionSuffix:" + versionSuffix);
			int versionFull = Integer.parseInt(versionSuffix);
			// System.out.println("--VersionFull:" + versionFull);
			int version = versionFull / 2;
			// System.out.println("--version:" + version);
			Boolean isAdd = false; // true if is Deleted
			if (versionFull % 2 == 0) {
				isAdd = true;
			}

			if (version > staticVersionQuery) {
				higherVersion = true;
			} else {
				// assume we have element1 and element2 variables as
				// a response
				String rowResult = soln.get("element1").toString() + " "
						+ soln.get("element2");

				// System.out.println("****** RowResult: " + rowResult);
				if (isAdd) {
					finalResults.add(rowResult);
					// System.out.println("ADDED");
				} else {
					finalResults.remove(rowResult);
					// System.out.println("DEL");
				}
			}
		}
		// System.out.println("TotalRows (up to version " + staticVersionQuery
		// + "):" + numRows);
		Iterator<String> it = finalResults.iterator();
		String res;

		while (it.hasNext()) {
			numfinalResults++;
			res = it.next();
			// element is the response
			// printStream.println(res);
			// System.out.println("count:" + count);
		}
		qexec.close();
		return numfinalResults;
	}

	static String readFile(String path, Charset encoding) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}

	private static Iterator<QuerySolution> orderedResultSet(
			ResultSet resultSet, final String sortingVariableName) {
		List<QuerySolution> list = new ArrayList<QuerySolution>();

		while (resultSet.hasNext()) {
			list.add(resultSet.nextSolution());
		}

		Collections.sort(list, new Comparator<QuerySolution>() {

			public int compare(QuerySolution a, QuerySolution b) {

				return a.getResource(sortingVariableName)
						.toString()
						.compareTo(
								b.getResource(sortingVariableName).toString());

			}
		});
		return list.iterator();
	}

	/**
	 * @param queryFile
	 * @param versionQuery
	 * @param printStream
	 * @param printStreamTime
	 * @param dataset
	 * @param queryString
	 */
	private static void verQuery(String queryFile, PrintStream printStream,
			PrintStream printStreamTime, Dataset dataset, String queryString) {
		Query query = QueryFactory.create(queryString);
		long startTime = System.currentTimeMillis();
		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
		try {
			ResultSet results = qexec.execSelect();

			HashSet<String> finalResults = new HashSet<String>();

			Iterator<QuerySolution> sortResults = orderedResultSet(results,
					"graph");
			int numRows = 0;
			int prevVersion = -1;
			while (sortResults.hasNext()) {
				numRows++;
				QuerySolution soln = sortResults.next();
				// assume we have a graph variable as a response
				String graphResponse = soln.getResource("graph").toString();
				System.out.println("--graphResponse:" + graphResponse);
				String versionSuffix = graphResponse.split("version")[1];
				System.out.println("--versionSuffix:" + versionSuffix);
				int versionFull = Integer.parseInt(versionSuffix);
				System.out.println("--VersionFull:" + versionFull);
				int version = versionFull / 2;
				System.out.println("--version:" + version);
				Boolean isAdd = false; // true if is Deleted
				if (versionFull % 2 == 0) {
					isAdd = true;
				}

				/*
				 * OUTPUT RESULTS OF THE PAST VERSION
				 */
				if (numRows > 1 && (version != prevVersion)) {
					Iterator<String> it = finalResults.iterator();
					String res;
					while (it.hasNext()) {
						res = it.next();
						// element is the response
						printStream.println(version + "," + res);
						// System.out.println("count:" + count);
					}
				}
				prevVersion = version;

				// assume we have element1 and element2
				// variables as
				// a response
				String rowResult = soln.get("element1").toString() + " "
						+ soln.get("element2");

				System.out.println("****** RowResult: " + rowResult);
				if (isAdd) {
					finalResults.add(rowResult);
					System.out.println("ADDED");
				} else {
					finalResults.remove(rowResult);
					System.out.println("DEL");
				}
				System.out.println("TotalRows (up to version " + version + "):"
						+ numRows);

			}
			/*
			 * OUTPUT LAST RESULTS
			 */
			if (numRows > 1) {
				Iterator<String> it = finalResults.iterator();
				String res;
				while (it.hasNext()) {
					res = it.next();
					// element is the response
					printStream.println(prevVersion + "," + res);
					// System.out.println("count:" + count);
				}
			}

		} finally {
			long endTime = System.currentTimeMillis();
			System.out.println("Time:" + (endTime - startTime));
			printStreamTime.println(queryFile + "," + (endTime - startTime));
			qexec.close();
		}
	}

	/**
	 * @param queryFile
	 * @param rol
	 * @param dataset
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static void bulkAllVerQuerying(String queryFile, String rol,
			Dataset dataset) throws FileNotFoundException, IOException {

		File inputFile = new File(queryFile);
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = "";

		TreeMap<Integer, DescriptiveStatistics> vStats = new TreeMap<Integer, DescriptiveStatistics>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			vStats.put(i, new DescriptiveStatistics());
		}
		DescriptiveStatistics total = new DescriptiveStatistics();

		while ((line = br.readLine()) != null) {

			/*
			 * warmup the system
			 */
			warmup(dataset);

			String[] parts = line.split(" ");

			String element = parts[0];

			String queryString = createQuery(rol, element);
			Query query = QueryFactory.create(queryString);
			long startTime = System.currentTimeMillis();
			QueryExecution qexec = QueryExecutionFactory.create(query, dataset);

			ResultSet results = qexec.execSelect();

			HashSet<String> finalResults = new HashSet<String>();

			Iterator<QuerySolution> sortResults = orderedResultSet(results,
					"graph");
			int numRows = 0;
			int prevVersion = -1;
			while (sortResults.hasNext()) {
				numRows++;
				QuerySolution soln = sortResults.next();
				// assume we have a graph variable as a response
				String graphResponse = soln.getResource("graph").toString();
				// System.out.println("--graphResponse:" + graphResponse);
				String versionSuffix = graphResponse.split("version")[1];
				// System.out.println("--versionSuffix:" + versionSuffix);
				int versionFull = Integer.parseInt(versionSuffix);
				// System.out.println("--VersionFull:" + versionFull);
				int version = versionFull / 2;
				// System.out.println("--version:" + version);
				Boolean isAdd = false; // true if is Deleted
				if (versionFull % 2 == 0) {
					isAdd = true;
				}

				/*
				 * OUTPUT RESULTS OF THE PAST VERSION
				 */
				if (numRows > 1 && (version != prevVersion)) {
					Iterator<String> it = finalResults.iterator();
					String res;
					while (it.hasNext()) {
						res = it.next();
						// element is the response
						// printStream.println(version + "," + res);
						// System.out.println("count:" + count);
					}
				}
				prevVersion = version;

				// assume we have element1 and element2
				// variables as
				// a response
				String rowResult = soln.get("element1").toString() + " "
						+ soln.get("element2");

				// System.out.println("****** RowResult: " + rowResult);
				if (isAdd) {
					finalResults.add(rowResult);
					// System.out.println("ADDED");
				} else {
					finalResults.remove(rowResult);
					// System.out.println("DEL");
				}
				// System.out.println("TotalRows (up to version " + version +
				// "):"
				// + numRows);

			}
			/*
			 * OUTPUT LAST RESULTS
			 */
			if (numRows > 1) {
				Iterator<String> it = finalResults.iterator();
				String res;
				while (it.hasNext()) {
					res = it.next();
					// element is the response
					// printStream.println(prevVersion + "," + res);
					// System.out.println("count:" + count);
				}
			}

			long endTime = System.currentTimeMillis();
			System.out.println("Time:" + (endTime - startTime));
			// printStreamTime.println(queryFile + "," + (endTime - startTime));
			qexec.close();
			total.addValue((endTime - startTime));

			// vStats.get(versionQuery).addValue((endTime-startTime));
		}

		PrintWriter pw = new PrintWriter(new File(outputDIR + "/res-dynver-"
				+ inputFile.getName()));
		pw.println("##name, min, mean, max, stddev, count");
		/*
		 * for(Entry<Integer, DescriptiveStatistics> ent: vStats.entrySet()){
		 * pw.println( ent.getKey()+" "+ ent.getValue().getMin()+" "+
		 * ent.getValue().getMean()+" "+ ent.getValue().getMax()+" "+
		 * ent.getValue().getStandardDeviation()+" "+ ent.getValue().getN() ); }
		 */
		pw.println("tot," + total.getMin() + "," + total.getMean() + ","
				+ total.getMax() + "," + total.getStandardDeviation() + ","
				+ total.getN());
		pw.close();
	}

	/**
	 * @param queryFile
	 * @param versionQuery
	 * @param printStream
	 * @param printStreamTime
	 * @param dataset
	 * @param queryString
	 */
	private static void warmup(Dataset dataset) {
		Query query = QueryFactory.create(createWarmupQuery());
		long startTime = System.currentTimeMillis();
		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);

		ResultSet results = qexec.execSelect();

		Iterator<QuerySolution> sortResults = orderedResultSet(results, "graph");
		int numRows = 0;

		while (sortResults.hasNext()) {
			numRows++;
			QuerySolution soln = sortResults.next();
			// assume we have a graph variable as a response
			String graphResponse = soln.getResource("graph").toString();
			System.out.println("--graphResponse:" + graphResponse);
			String versionSuffix = graphResponse.split("version")[1];
			System.out.println("--versionSuffix:" + versionSuffix);
			int versionFull = Integer.parseInt(versionSuffix);
			System.out.println("--VersionFull:" + versionFull);
			int version = versionFull / 2;
			System.out.println("--version:" + version);
			Boolean isAdd = false; // true if is Deleted
			if (versionFull % 2 == 0) {
				isAdd = true;
			}

			System.out.println("TotalRows (up to version " + version + "):"
					+ numRows);

		}

		long endTime = System.currentTimeMillis();
		System.out.println("Warmup Time:" + (endTime - startTime));

		qexec.close();

	}

	private static String createWarmupQuery() {
		String queryString = "SELECT ?element1 ?element2 ?element3 ?graph WHERE { "
				+ "GRAPH ?graph{" + " ?element1 ?element2 ?element3 ."

				+ "}}" + "LIMIT 100";

		return queryString;
	}
}
