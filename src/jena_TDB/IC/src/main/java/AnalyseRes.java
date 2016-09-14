import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.tdb.TDBFactory;

import arq.arq;


public class AnalyseRes {

	public static void main(String[] args) throws ParseException, IOException {
		// TODO Auto-generated method stub
		Options options = new Options();
		options.addOption("d",true, "directory for tdb files");
		options.addOption("f", true,"list of URLs");
		options.addOption("o", true,"output directory");
		options.addOption("t", true,"allowed values s,p,o ");
		
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse( options, args);
		
		File tdbFolder = new File(cmd.getOptionValue("d"));
		File resFile = new File(cmd.getOptionValue("f"));
		String type = cmd.getOptionValue("t");
		File outDir = new File(cmd.getOptionValue("o"),type);
		
		if(!outDir.exists()) outDir.mkdirs();
		
		long start = System.currentTimeMillis();
		Map<Integer, Dataset> datasets =  initDatasets(tdbFolder);
		System.out.println("Loaded "+datasets.size()+" in "+(System.currentTimeMillis()-start)+" ms");
		
		InputStream is = new FileInputStream(resFile);
		if(resFile.getName().endsWith(".gz")){
			is = new GZIPInputStream(is);
		}
		Scanner s = new Scanner(is);
		int c =0;
		while(s.hasNextLine()){
			String line = s.nextLine();
					
			String[] uri = line.trim().split(" ");
//			System.out.println(uri[0]);
			if(uri.length==2 && Integer.valueOf(uri[0])==58 ){
				c++;
				System.out.println(uri[0]+" "+uri[1]);
				
				ResourceResult r = new ResourceResult(uri[1], type);
				start = System.currentTimeMillis();
				analyseQuery(r, datasets, outDir);
				long end = System.currentTimeMillis();
				
				System.out.println("Time elapsed "+(end-start)+" ms");
				
//				if(c%10==0)
//					System.exit(-1);
			}
		}
		
		
		
		
//		QueryExecution qe = QueryExecutionFactory.create(query, dataset) ;
	}

	private static void analyseQuery(ResourceResult r,
			Map<Integer, Dataset> datasets, File outDir) {
		
		Set<QueryThread> a = new HashSet<QueryThread>();
		for(Entry<Integer, Dataset>ent: datasets.entrySet()){
			
			QueryThread an = new QueryThread(ent.getKey(), ent.getValue(), r);
			an.start();
			a.add(an);
		}
		for(QueryThread an: a){
			try {
				an.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		r.analyse();
		try {
			r.print(outDir);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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
