import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.hp.hpl.jena.query.QuerySolution;


public class ResourceResult {

	private String _u;
	private String _t;
	private TreeMap<Integer, TreeSet<QuerySolution>> _res;
	private TreeMap<Integer, Double> _dyn;
	private DescriptiveStatistics _dynStats;
	private DescriptiveStatistics _cardStats;


	public ResourceResult(String uri, String t) {
		_u = uri;
		if(!_u.startsWith("<")){
			_u="<"+_u+">";
		}
		_t=t.trim();
		_res= new TreeMap<Integer, TreeSet<QuerySolution>>();
	}
	
	
	public String createQuery() {
		String q = "SELECT DISTINCT ?1 ?2 WHERE{";
		switch(_t){
			case "s":
				q+= _u+" ?1 ?2 ."; break;
			case "p":
				q+= "?1 "+_u+" ?2 .";break;
			case "o":
				q+= "?1 ?2 "+_u;break;
		}
		return q+="}";
	}


	public void update(Integer _s, TreeSet<QuerySolution> sol) {
		_res.put(_s, sol);
	}
	
	public void print(File outDir) throws IOException{
		File dir = new File(outDir,"data");
		dir.mkdirs();
		PrintWriter pw = new PrintWriter(new File(dir, URLEncoder.encode(_u.replaceAll("<", "").replaceAll(">", ""))+".stats"));
		pw.println("1 "+_res.get(1).size()+" 0.0");
		
		for(int i=1; i<_res.size(); i++){
			pw.println(i+" "+_res.get(i+1).size()+" "+_dyn.get(i+1));
		}
		pw.close();
		FileWriter fw = new FileWriter(new File(outDir, "all.stats"),true);
		fw.write(_u+" "+
				_cardStats.getMin()+" "+
				_cardStats.getMean()+" "+
				_cardStats.getMax()+" "+
				_cardStats.getStandardDeviation()+" "+
				_dynStats.getMin()+" "+
				_dynStats.getMean()+" "+
				_dynStats.getMax()+" "+
				_dynStats.getStandardDeviation()+"\n"
				);
		fw.close();
		
	}


	public void analyse() {
		
		_dynStats= new DescriptiveStatistics();
		_cardStats= new DescriptiveStatistics();
		
		_cardStats.addValue(_res.get(1).size());
		_dyn= new TreeMap<Integer, Double>();
		for(int i=1; i<_res.size(); i++){
			_cardStats.addValue(_res.get(i+1).size());
			
			TreeSet<QuerySolution> a = _res.get(i);
			TreeSet<QuerySolution> b = _res.get(i+1);
			if(a.size()==0){
				System.out.println("WARN: empty results for snapshot "+i);
			}if(b.size()==0){
				System.out.println("WARN: empty results for snapshot "+(i+1));
			}
			
			double dyn=0;
			if(a.size() >= b.size()){ 
				dyn=compare(b,a);
			}else{ 
				dyn=compare(a,b);
			}
			
			if(dyn == Double.NaN){
				System.out.println("WARN: NAN for "+i+"-"+(i+1));
			}else{
				_dyn.put(i, dyn);
			}
			
//			System.out.println("____________________");
//			if(dyn>0){
//				for(QuerySolution s: a){
//					System.out.println((i)+" "+s);
//					
//				}for(QuerySolution s: b){
//					System.out.println((i+1)+" "+s+ " "+a.contains(s));
//				}
//			}
//			System.out.println(dyn);
			_dynStats.addValue(dyn);
		}
	}


	private double compare(TreeSet<QuerySolution> b, TreeSet<QuerySolution> a) {
		int total = b.size()+a.size();
		int same=0;
		for(QuerySolution sol: b){
			if(a.contains(sol)){
				same++;
			}
		}
		double d = (a.size()-same)+(b.size()-same);
		return (d/total);
	}


	public Map<Integer, TreeSet<QuerySolution>> getResults() {
		return _res;
	}
}
