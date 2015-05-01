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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.util.FileManager;

public class CB_Archive {

	private static int TOTALVERSIONS = 58;

	private static String outputDIR = "results";

	public static void main(String[] args) throws FileNotFoundException,
			InterruptedException, ExecutionException {

		String dirTDBs = null;
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

			/*
			 * We assume the following structure: - 0 .. 57 ---- add/ ---- del/
			 */

			Option inputDirOpt = new Option("d", "dir", true,
					"DIR to load TDBs");
			inputDirOpt.setRequired(true);
			options.addOption(inputDirOpt);

			Option queryOpt = new Option("q", "query", true,
					"SPARQL query to process");
			queryOpt.setRequired(false);
			options.addOption(queryOpt);

			Option filequeryOpt = new Option("f", "file with queries", true,
					"queries to process");
			filequeryOpt.setRequired(false);
			options.addOption(filequeryOpt);

			Option fileDynqueryOpt = new Option("F",
					"file with dynamic queries", true,
					"dynamic queries to process");
			fileDynqueryOpt.setRequired(false);
			options.addOption(fileDynqueryOpt);

			Option queryCatOpt = new Option("c", "category", true,
					"Query category: mat | diff | ver ");
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

			Option jumpCatOpt = new Option("j", "jump", true,
					"Jump step for the diff: e.g. 5 (0-5,0-10..)");
			jumpCatOpt.setRequired(false);
			options.addOption(jumpCatOpt);

			Option helpOpt = new Option("h", "help", false, "Shows help");
			helpOpt.setRequired(false);
			options.addOption(helpOpt);

			// Parse input arguments
			CommandLineParser cliParser = new BasicParser();
			CommandLine cmdLine = cliParser.parse(options, args);

			// Read arguments

			if (cmdLine.hasOption("d")) {
				dirTDBs = cmdLine.getOptionValue("d");
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
			Logger.getLogger(CB_Archive.class.getName()).log(Level.SEVERE,
					null, e);
		} catch (Exception e) {
			e.printStackTrace();
		}

		OutputStream os = new FileOutputStream(outputResults);
		OutputStream os_time = new FileOutputStream(outputTime);
		PrintStream printStream = new PrintStream(os);
		PrintStream printStreamTime = new PrintStream(os_time);

		// Initialize Jena
		FileManager fm = FileManager.get();
		fm.addLocatorClassLoader(CB_Archive.class.getClassLoader());

		/*
		 * Load all datasets
		 */
		Dataset dataset_adds[] = new Dataset[TOTALVERSIONS];
		Dataset dataset_dels[] = new Dataset[TOTALVERSIONS];

		File folder = new File(dirTDBs);
		for (File fileEntry : folder.listFiles()) {
			int fileVersion = Integer.parseInt(fileEntry.getName());
			System.out.println("... Loading TDB version" + fileVersion
					+ " as ADD");
			dataset_adds[fileVersion] = TDBFactory.createDataset(dirTDBs + "/"
					+ fileVersion + "/add/");
			System.out.println("... Loading TDB version" + fileVersion
					+ " as DEL");
			dataset_dels[fileVersion] = TDBFactory.createDataset(dirTDBs + "/"
					+ fileVersion + "/del/");
		}

		CB_Archive program = new CB_Archive();

		try {
			System.out.println("Query is " + queryCategory);
			if (queryCategory.equalsIgnoreCase("mat")) {

				/*
				 * ONE QUERY
				 */
				if (!bulkQueries) {
					String queryString = readFile(queryFile,
							StandardCharsets.UTF_8);
					System.out.println("Query is '" + queryCategory
							+ "' at version " + versionQuery);
					System.out.println(queryString);

					program.matQuery(queryFile, versionQuery, printStream,
							printStreamTime, dataset_adds, dataset_dels,
							queryString);
				}
				/*
				 * FILE CONTAINS SEVERAL QUERIES
				 */
				else {
					if (queryFile != null)
						program.bulkMatQuerying(queryFile, rol, dataset_adds,
								dataset_dels);
					else
						program.bulkAllMatQuerying(queryFileDynamic, rol,
								dataset_adds, dataset_dels);
				}
			} else if (queryCategory.equalsIgnoreCase("diff")) {

				if (!bulkQueries) {
					System.out.println("Diff between version " + versionQuery
							+ " and version " + postversionQuery);
					String queryString = readFile(queryFile,
							StandardCharsets.UTF_8);

					System.out.println(queryString);
					program.diffQuerying(queryFile, versionQuery,
							postversionQuery, printStream, printStreamTime,
							dataset_adds, dataset_dels, queryString);
				} else {
					System.out.println("Diff of all versions ");
					if (jump > 0)
						System.out.println("     with jump " + jump);
					program.bulkAlldiffQuerying(queryFileDynamic, rol,
							dataset_adds, dataset_dels, jump);
				}
			}

			else if (queryCategory.equalsIgnoreCase("ver")) {
				if (!bulkQueries) {
					String queryString = readFile(queryFile,
							StandardCharsets.UTF_8);
					System.out.println(queryString);
					program.verQuery(queryFile, printStream, printStreamTime,
							dataset_adds, dataset_dels, queryString);
				} else {
					program.bulkAllVerQuerying(queryFileDynamic, rol,
							dataset_adds, dataset_dels);
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		for (int i = 0; i < 58; i++) {
			dataset_adds[i].end();
			dataset_dels[i].end();
		}
		printStream.close();
		printStreamTime.close();
	}

	/**
	 * @param queryFile
	 * @param startVersionQuery
	 * @param endVersionQuery
	 * @param printStream
	 * @param printStreamTime
	 * @param dataset
	 * @param queryString
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private void diffQuerying(String queryFile, int startVersionQuery,
			int endVersionQuery, PrintStream printStream,
			PrintStream printStreamTime, Dataset[] dataset_adds,
			Dataset[] dataset_dels, String queryString)
			throws InterruptedException, ExecutionException {
		Query query = QueryFactory.create(queryString);
		long startTime = System.currentTimeMillis();

		ResultSet[] results_adds = new ResultSet[TOTALVERSIONS];
		ResultSet[] results_dels = new ResultSet[TOTALVERSIONS];

		/**
		 * START PARALELL
		 */

		Collection<Callable<QueryResult>> tasks = new ArrayList<>();
		// // for the (initial version +1) up to the post version
		// // Note that it is +1 in order to compute the difference with the
		// // following one

		for (int i = startVersionQuery + 1; i <= endVersionQuery; i++) {

			tasks.add(new Task(query, dataset_adds[i], i, true));
			tasks.add(new Task(query, dataset_dels[i], i, false));
		}
		ExecutorService executor = Executors.newFixedThreadPool(TOTALVERSIONS);
		List<Future<QueryResult>> results = executor.invokeAll(tasks);

		/**
		 * END PARALELL
		 */

		for (Future<QueryResult> result : results) {
			QueryResult res = result.get();
			System.out.println("version:" + res.version);
			System.out.println("version:" + res.sol.hasNext());
			if (res.isAdd)
				results_adds[res.version] = res.sol;
			else
				results_dels[res.version] = res.sol;
		}
		/**
		 * END PARALELL
		 */

		HashSet<String> finalResultsAdd = new HashSet<String>();
		HashSet<String> finalResultsDel = new HashSet<String>();

		for (int i = (startVersionQuery + 1); i <= endVersionQuery; i++) {

			while (results_adds[i].hasNext()) {
				QuerySolution soln = results_adds[i].next();
				String rowResult = soln.get("element1").toString() + " "
						+ soln.get("element2");

				System.out.println("****** RowResult ADD: " + rowResult);

				if (!finalResultsDel.remove(rowResult))
					finalResultsAdd.add(rowResult);

			}
			while (results_dels[i].hasNext()) {
				QuerySolution soln = results_dels[i].next();
				String rowResult = soln.get("element1").toString() + " "
						+ soln.get("element2");

				System.out.println("****** RowResult DEL: " + rowResult);

				if (!finalResultsAdd.remove(rowResult))
					finalResultsDel.add(rowResult);

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
		executor.shutdown(); // always reclaim resources
		long endTime = System.currentTimeMillis();
		System.out.println("Time:" + (endTime - startTime));
		printStreamTime.println(queryFile + "," + (endTime - startTime));

		// for (int i=0;i<TOTALVERSIONS;i++){
		// qexec_add[i].close();
		// qexec_del[i].close();
		// }

	}

	/**
	 * @param queryFile
	 * @param dataset_adds
	 * @param dataset_dels
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	private void bulkAlldiffQuerying(String queryFile, String rol,
			Dataset[] dataset_adds, Dataset[] dataset_dels, int jump)
			throws InterruptedException, ExecutionException, IOException {

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
			warmup(dataset_adds, dataset_dels);

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

				Query query = QueryFactory.create(queryString);

				long startTime = System.currentTimeMillis();

				/**
				 * START PARALELL
				 */

				TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, CB_Archive.QueryResult>();
				TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, CB_Archive.QueryResult>();

				Set<TaskThread> a = new HashSet<TaskThread>();
				for (int i = versionQuery + 1; i <= postversionQuery; i++) {
					TaskThread task_add = new TaskThread(query,
							dataset_adds[i], i, true, results_adds);
					TaskThread task_del = new TaskThread(query,
							dataset_dels[i], i, false, results_dels);
					task_add.start();
					a.add(task_add);
					task_del.start();
					a.add(task_del);
				}
				for (TaskThread an : a) {
					try {
						an.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				/**
				 * END PARALELL
				 */

				HashSet<String> finalResultsAdd = new HashSet<String>();
				HashSet<String> finalResultsDel = new HashSet<String>();

				for (int i = (versionQuery + 1); i <= postversionQuery; i++) {

					while (results_adds.get(i).sol.hasNext()) {
						QuerySolution soln = results_adds.get(i).sol.next();
						String rowResult = soln.get("element1").toString()
								+ " " + soln.get("element2");

						// System.out.println("****** RowResult ADD: " +
						// rowResult);

						if (!finalResultsDel.remove(rowResult))
							finalResultsAdd.add(rowResult);

					}
					while (results_dels.get(i).sol.hasNext()) {
						QuerySolution soln = results_dels.get(i).sol.next();
						String rowResult = soln.get("element1").toString()
								+ " " + soln.get("element2");

						// System.out.println("****** RowResult DEL: " +
						// rowResult);

						if (!finalResultsAdd.remove(rowResult))
							finalResultsDel.add(rowResult);

					}
				}

				Iterator<String> it = finalResultsAdd.iterator();
				String res;
				while (it.hasNext()) {
					res = it.next();
					// element is the response
					// printStream.println(">" + res);
					// System.out.println(">" + res);
					// System.out.println("count:" + count);
				}
				it = finalResultsDel.iterator();
				while (it.hasNext()) {
					res = it.next();
					// element is the response
					// printStream.println("<" + res);
					// System.out.println("<" + res);
					// System.out.println("count:" + count);
				}
				long endTime = System.currentTimeMillis();
				System.out.println("Time:" + (endTime - startTime));
				total.addValue((endTime - startTime));
				vStats.get(index).addValue((endTime - startTime));

				

				// printStreamTime.println(queryFile + "," + (endTime -
				// startTime));

			
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
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	private void matQuery(String queryFile, int versionQuery,
			PrintStream printStream, PrintStream printStreamTime,
			Dataset[] dataset_adds, Dataset[] dataset_dels, String queryString)
			throws InterruptedException, ExecutionException {
		Query query = QueryFactory.create(queryString);

		long startTime = System.currentTimeMillis();

		materializeQuery(dataset_adds, dataset_dels, versionQuery, query);

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
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	private void bulkMatQuerying(String queryFile, String rol,
			Dataset[] dataset_adds, Dataset[] dataset_dels)
			throws FileNotFoundException, IOException, InterruptedException,
			ExecutionException {

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

			long startTime = System.currentTimeMillis();
			Query query = QueryFactory.create(queryString);

			int numFinalResults = materializeQuery(dataset_adds, dataset_dels,
					staticVersionQuery, query);

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

	/**
	 * @param queryFile
	 * @param rol
	 * @param dataset
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	private void bulkAllMatQuerying(String queryFile, String rol,
			Dataset[] dataset_adds, Dataset[] dataset_dels)
			throws FileNotFoundException, IOException, InterruptedException,
			ExecutionException {
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

			/*
			 * warmup the system
			 */
			warmup(dataset_adds, dataset_dels);

			String queryString = createQuery(rol, element);
			for (int i = 0; i < TOTALVERSIONS; i++) {
				System.out.println("Query at version " + i);
				System.out.println(queryString);

				Query query = QueryFactory.create(queryString);
				long startTime = System.currentTimeMillis();

				int numFinalResults = materializeQuery(dataset_adds,
						dataset_dels, i, query);

				long endTime = System.currentTimeMillis();
				System.out.println("Time:" + (endTime - startTime));
				vStats.get(i).addValue((endTime - startTime));

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
	 * @param dataset_adds
	 * @param dataset_dels
	 * @param staticVersionQuery
	 * @param query
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private int materializeQuery(Dataset[] dataset_adds,
			Dataset[] dataset_dels, int staticVersionQuery, Query query)
			throws InterruptedException, ExecutionException {

		/**
		 * START PARALELL
		 */

		TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, CB_Archive.QueryResult>();
		TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, CB_Archive.QueryResult>();

		Set<TaskThread> a = new HashSet<TaskThread>();
		for (int i = 0; i <= staticVersionQuery; i++) {
			TaskThread task_add = new TaskThread(query, dataset_adds[i], i,
					true, results_adds);
			TaskThread task_del = new TaskThread(query, dataset_dels[i], i,
					false, results_dels);
			task_add.start();
			a.add(task_add);
			task_del.start();
			a.add(task_del);
		}
		for (TaskThread an : a) {
			try {
				an.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/**
		 * END PARALELL
		 */

		HashSet<String> finalResults = new HashSet<String>();

		for (int i = 0; i <= staticVersionQuery; i++) {
			// System.out.println("Iterating results " + i);
			while (results_adds.get(i).sol.hasNext()) {
				QuerySolution soln = results_adds.get(i).sol.next();
				// System.out.println("++ ADDED in Version:" + i);
				String rowResult = soln.get("element1").toString() + " "
						+ soln.get("element2");

				// System.out.println("****** RowResult: " + rowResult);
				finalResults.add(rowResult);
			}
			while (results_dels.get(i).sol.hasNext()) {
				QuerySolution soln = results_dels.get(i).sol.next();
				// System.out.println("-- DEL in Version:" + i);
				String rowResult = soln.get("element1").toString() + " "
						+ soln.get("element2");

				// System.out.println("****** RowResult: " + rowResult);
				finalResults.remove(rowResult);
			}
		}
		Iterator<String> it = finalResults.iterator();
		String res;

		int numFinalResults = 0;
		while (it.hasNext()) {
			numFinalResults++;
			res = it.next();
			// element is the response
			// printStream.println(res);

			// System.out.println("res:" + res);
			// System.out.println("count:" + count);
		}

		

		return numFinalResults;


	}

	static String readFile(String path, Charset encoding) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}

	/**
	 * @param queryFile
	 * @param versionQuery
	 * @param printStream
	 * @param printStreamTime
	 * @param dataset
	 * @param queryString
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private void verQuery(String queryFile, PrintStream printStream,
			PrintStream printStreamTime, Dataset[] dataset_adds,
			Dataset[] dataset_dels, String queryString)
			throws InterruptedException, ExecutionException {
		Query query = QueryFactory.create(queryString);
		long startTime = System.currentTimeMillis();
		ResultSet[] results_adds = new ResultSet[TOTALVERSIONS];
		ResultSet[] results_dels = new ResultSet[TOTALVERSIONS];

		/**
		 * START PARALELL
		 */

		Collection<Callable<QueryResult>> tasks = new ArrayList<>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			tasks.add(new Task(query, dataset_adds[i], i, true));
			tasks.add(new Task(query, dataset_dels[i], i, false));
		}
		ExecutorService executor = Executors.newFixedThreadPool(TOTALVERSIONS);
		List<Future<QueryResult>> results = executor.invokeAll(tasks);

		/**
		 * END PARALELL
		 */
		HashSet<String> finalResults = new HashSet<String>();
		for (Future<QueryResult> result : results) {
			QueryResult res = result.get();
			System.out.println("version:" + res.version);
			System.out.println("version:" + res.sol.hasNext());
			if (res.isAdd)
				results_adds[res.version] = res.sol;
			else
				results_dels[res.version] = res.sol;
		}
		// for all versions
		for (int i = 0; i < TOTALVERSIONS; i++) {

			System.out.println("computing results ADD for version " + i);
			while (results_adds[i].hasNext()) {
				QuerySolution soln = results_adds[i].next();
				String rowResult = soln.get("element1").toString() + " "
						+ soln.get("element2");

				finalResults.add(rowResult);
			}
			System.out.println("computing results DEL for version " + i);
			while (results_dels[i].hasNext()) {
				QuerySolution soln = results_dels[i].next();
				String rowResult = soln.get("element1").toString() + " "
						+ soln.get("element2");

				finalResults.remove(rowResult);
			}

			/*
			 * OUTPUT RESULTS OF THE CURRENT VERSION
			 */

			Iterator<String> it = finalResults.iterator();
			String res;
			while (it.hasNext()) {
				res = it.next();
				// element is the response
				printStream.println(i + "," + res);
				// System.out.println("count:" + count);
			}
		}

		executor.shutdown(); // always reclaim resources

		long endTime = System.currentTimeMillis();
		System.out.println("Time:" + (endTime - startTime));
		printStreamTime.println(queryFile + "," + (endTime - startTime));

		// for (int i=0;i<TOTALVERSIONS;i++){
		// qexec_add[i].close();
		// qexec_del[i].close();
		// }
	}

	/**
	 * @param queryFile
	 * @param dataset
	 * @param queryString
	 * @throws IOException
	 */
	private void bulkAllVerQuerying(String queryFile, String rol,
			Dataset[] dataset_adds, Dataset[] dataset_dels)
			throws InterruptedException, ExecutionException, IOException {

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
			warmup(dataset_adds, dataset_dels);

			String queryString = createQuery(rol, element);
			Query query = QueryFactory.create(queryString);

			long startTime = System.currentTimeMillis();

			/**
			 * START PARALELL
			 */

			TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, CB_Archive.QueryResult>();
			TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, CB_Archive.QueryResult>();

			Set<TaskThread> a = new HashSet<TaskThread>();
			for (int i = 0; i < TOTALVERSIONS; i++) {
				TaskThread task_add = new TaskThread(query, dataset_adds[i], i,
						true, results_adds);
				TaskThread task_del = new TaskThread(query, dataset_dels[i], i,
						false, results_dels);
				task_add.start();
				a.add(task_add);
				task_del.start();
				a.add(task_del);
			}
			for (TaskThread an : a) {
				try {
					an.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			/**
			 * END PARALELL
			 */

			HashSet<String> finalResults = new HashSet<String>();

			// for all versions
			for (int i = 0; i < TOTALVERSIONS; i++) {

				System.out.println("computing results ADD for version " + i);
				while (results_adds.get(i).sol.hasNext()) {
					QuerySolution soln = results_adds.get(i).sol.next();
					String rowResult = soln.get("element1").toString() + " "
							+ soln.get("element2");

					finalResults.add(rowResult);
				}
				System.out.println("computing results ADD for version " + i);
				while (results_dels.get(i).sol.hasNext()) {
					QuerySolution soln = results_dels.get(i).sol.next();
					String rowResult = soln.get("element1").toString() + " "
							+ soln.get("element2");

					finalResults.remove(rowResult);
				}

				/*
				 * OUTPUT RESULTS OF THE CURRENT VERSION
				 */

				Iterator<String> it = finalResults.iterator();
				String res;
				while (it.hasNext()) {
					res = it.next();
					// element is the response
					// printStream.println(i + "," + res);

					// System.out.println("i:" + res);
					// System.out.println("count:" + count);
				}
			}
			long endTime = System.currentTimeMillis();
			System.out.println("Time:" + (endTime - startTime));
			total.addValue((endTime - startTime));
			
			// printStreamTime.println(queryFile + "," + (endTime - startTime));

			
		}

		PrintWriter pw = new PrintWriter(new File(outputDIR + "/res-dynver-"
				+ inputFile.getName()));
		pw.println("##name, min, mean, max, stddev, count");
		pw.println("tot," + total.getMin() + "," + total.getMean() + ","
				+ total.getMax() + "," + total.getStandardDeviation() + ","
				+ total.getN());
		pw.close();
	}

	/** Try to ping a URL. Return true only if successful. */
	private final class Task implements Callable<QueryResult> {

		Task(Query query, Dataset dataset, int version, Boolean isAdd) {
			this.query = query;
			this.dataset = dataset;
			this.version = version;
			this.isAdd = isAdd;
		}

		/** Access a URL, and see if you get a healthy response. */
		@Override
		public QueryResult call() throws Exception {
			QueryResult ret = new QueryResult();
			ret.ex = QueryExecutionFactory.create(query, dataset);
			ret.sol = ret.ex.execSelect();
			ret.version = version;
			ret.isAdd = isAdd;
			return ret;
		}

		private final Query query;
		private final Dataset dataset;
		private final int version;
		private final Boolean isAdd; // otherwise is del
	}

	private static final class QueryResult {
		QueryExecution ex;
		ResultSet sol;
		int version;
		Boolean isAdd; // otherwise is del
	}

	/** Try to ping a URL. Return true only if successful. */
	private final class TaskThread extends Thread {

		TreeMap<Integer, QueryResult> res;

		public TaskThread(Query query, Dataset dataset, int version,
				Boolean isAdd, TreeMap<Integer, QueryResult> res) {
			this.query = query;
			this.dataset = dataset;
			this.version = version;
			this.isAdd = isAdd;
			this.res = res;
		}

		@Override
		public synchronized void start() {
			QueryResult ret = new QueryResult();
			ret.ex = QueryExecutionFactory.create(query, dataset);
			ret.sol = ret.ex.execSelect();
			ret.version = version;
			ret.isAdd = isAdd;
			this.res.put(version, ret);
		}

		private final Query query;
		private final Dataset dataset;
		private final int version;
		private final Boolean isAdd; // otherwise is del
	}

	private static String createQuery(String rol, String element) {
		String queryString = "SELECT ?element1 ?element2 WHERE { ";
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
		queryString = queryString + "}";

		return queryString;
	}

	/**
	 * @param queryFile
	 * @param versionQuery
	 * @param printStream
	 * @param printStreamTime
	 * @param dataset
	 * @param queryString
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	private void warmup(Dataset[] dataset_adds, Dataset[] dataset_dels)
			throws InterruptedException, ExecutionException {
		Query query = QueryFactory.create(createWarmupQuery());
		long startTime = System.currentTimeMillis();

		/**
		 * START PARALELL
		 */

		TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, CB_Archive.QueryResult>();
		TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, CB_Archive.QueryResult>();

		Set<TaskThread> a = new HashSet<TaskThread>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			TaskThread task_add = new TaskThread(query, dataset_adds[i], i,
					true, results_adds);
			TaskThread task_del = new TaskThread(query, dataset_dels[i], i,
					false, results_dels);
			task_add.start();
			a.add(task_add);
			task_del.start();
			a.add(task_del);
		}
		for (TaskThread an : a) {
			try {
				an.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/**
		 * END PARALELL
		 */
		HashSet<String> finalResults = new HashSet<String>();
		

		for (int i = 0; i < TOTALVERSIONS; i++) {
			// System.out.println("Iterating results " + i);
			while (results_adds.get(i).sol.hasNext()) {
				QuerySolution soln = results_adds.get(i).sol.next();
				// System.out.println("++ ADDED in Version:" + i);
				String rowResult = soln.get("element1").toString() + " "
						+ soln.get("element2");

				// System.out.println("****** RowResult: " + rowResult);
				finalResults.add(rowResult);
			}
			while (results_dels.get(i).sol.hasNext()) {
				QuerySolution soln = results_dels.get(i).sol.next();
				// System.out.println("-- DEL in Version:" + i);
				String rowResult = soln.get("element1").toString() + " "
						+ soln.get("element2");

				// System.out.println("****** RowResult: " + rowResult);
				finalResults.remove(rowResult);
			}
		}

		long endTime = System.currentTimeMillis();
		System.out.println("Warmup Time:" + (endTime - startTime));

	}

	private static String createWarmupQuery() {
		String queryString = "SELECT ?element1 ?element2 ?element3 WHERE { "
				+ " ?element1 ?element2 ?element3 ."

				+ "}" + "LIMIT 100";

		return queryString;
	}
}
