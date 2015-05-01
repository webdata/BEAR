import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
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

import arq.arq;


public class StaticMAT {

	private static Options options;
	
	private static CommandLine parseOption(String[] args) {
		options = new Options();
		options.addOption("h", "help", false, "show help.");
		options.addOption("d",true, "tdb folder");
		options.addOption("i", true,"query file");
		options.addOption("t", true,"type [s, p, o]");
		options.addOption("o", true,"output dir");
		
		CommandLineParser parser = new BasicParser();
		try {
			CommandLine cmd = parser.parse( options, args);
			
			if(args.length!=8 ||cmd.hasOption("h") || cmd.hasOption("help")) help();
			return cmd;
		} catch (ParseException e1) {
			System.err.println( "Parsing failed.  Reason: " + e1.getMessage() );
			help(); return null;
		}
	}
	private static void help() {
		HelpFormatter formater = new HelpFormatter();
		formater.printHelp("Main", options);
		System.exit(0);
	}
	
	

	
	public static void main(String[] args) throws IOException {
		CommandLine cmd = parseOption(args);
		
		File tdbFolder = new File(cmd.getOptionValue("d"));
		File input = new File(cmd.getOptionValue("i"));
		String type = cmd.getOptionValue("t");
		File outDir = new File(cmd.getOptionValue("o"),type);
		
		long start = System.currentTimeMillis();
		Map<Integer, Dataset> datasets =  initDatasets(tdbFolder);
		System.out.println("Loaded "+datasets.size()+" in "+(System.currentTimeMillis()-start)+" ms");
		
		if(!outDir.exists()) outDir.mkdirs();
//		File res= new File(outDir,"results");
//		res.mkdir();
		
		
		PrintWriter resultstream = new PrintWriter(new File(outDir, "static-mat-ic-"+type+"-times.csv"));
		
		TreeMap<Integer, DescriptiveStatistics> vStats= new TreeMap<Integer, DescriptiveStatistics>();
		for(int i=1; i <=datasets.size(); i++){
			vStats.put(i, new DescriptiveStatistics());
		}
		
		InputStream is = new FileInputStream(input);
		if(input.getName().endsWith(".gz")){
			is = new GZIPInputStream(is);
		}
		Scanner s = new Scanner(is);
		int c =0;
		while(s.hasNextLine()){
			String line = s.nextLine();
					
			String[] val = line.trim().split(",");
			//54,2,4.5537665147750256E-8,384bb13f812f69bc1cb4e135f9afb4fb
			
			if(val.length==4){
				c++;
				Integer version = Integer.valueOf(val[0])+1;
				ResourceResult r = new ResourceResult(val[3], type);
				String query = r.createQuery();
				
				System.out.println("Querying "+val[3]+" at "+type+" in version "+version);
				start = System.currentTimeMillis();
				TreeSet<QuerySolution> sol = QueryType.execute(query, datasets.get(version));
				long end = System.currentTimeMillis();
				vStats.get(version).addValue((end-start));
				resultstream.println(version+","+sol.size()+","+(end-start)+","+val[3]);
				System.out.println("Query snap "+version+" with "+type+" "+val[3]);
				System.out.println("Time elapsed "+(end-start)+" ms");
			}else{
				System.out.println("Dropping "+line);
			}
		}
		PrintWriter pw = new PrintWriter(new File(outDir,"res-statmat-ic-"+input.getName()));
		pw.println("#key, min, mean, max, stddev, count");
		for(Entry<Integer, DescriptiveStatistics>ent: vStats.entrySet()){
			pw.println(	(ent.getKey()-1)+" "+
						ent.getValue().getMin()+" "+
						ent.getValue().getMean()+" "+
						ent.getValue().getMax()+" "+
						ent.getValue().getStandardDeviation()+" "+
						ent.getValue().getN()
					);
		}
		pw.close();
	}

	private static Map<Integer, Dataset> initDatasets(File tdbFolder) {
		if(!tdbFolder.isDirectory()) throw new RuntimeException("tdbfolder "+tdbFolder+" is not a directory");
		
		System.out.println("Parsing files in "+tdbFolder);
		File[] files = tdbFolder.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.contains(".tdb");
			}
		});

		Map<Integer, Dataset> d = new TreeMap<Integer, Dataset>();
		
		Arrays.sort(files, new Comparator<File>(){
		    public int compare(File f1, File f2)
		    {
		    	Integer a = Integer.valueOf(f1.getName().substring(0,f1.getName().indexOf(".")));
		    	Integer b = Integer.valueOf(f2.getName().substring(0,f2.getName().indexOf(".")));
		        return a.compareTo(b);
		    } });
		for(File f: files){
			System.out.println("loading "+f);
			Integer i = Integer.valueOf(f.getName().substring(0,f.getName().indexOf(".")));
			Dataset dataset = TDBFactory.createDataset(f.getAbsolutePath()) ;
			d.put(i,dataset);
		}
		
		return d;

	}

}
