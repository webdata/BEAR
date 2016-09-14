import java.util.Comparator;
import java.util.HashSet;
import java.util.TreeSet;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;


public class QueryThread extends Thread {

	private Integer _s;
	private Dataset _d;
	private ResourceResult _r;
	private String _q;

	public QueryThread(Integer s, Dataset d, ResourceResult r) {
//		System.out.println("Init Analyser "+s);
		this(s,d,r,r.createQuery());
		
	}
	public QueryThread(Integer s, Dataset d, ResourceResult r, String query) {
//		System.out.println("Init Analyser "+s);
		_s=s;
		_d=d;
		_r=r;
		_q = query;
	}

	@Override
	public synchronized void start() {
		
		Query query = QueryFactory.create(_q);
		long startTime = System.currentTimeMillis();
		QueryExecution qexec = QueryExecutionFactory.create(query, _d);
		boolean error=false;
		TreeSet<QuerySolution> sol = new TreeSet<QuerySolution>(new QuerySolutionComparator());
		try {
			
			ResultSet results = qexec.execSelect();
			while (results.hasNext()) {
				QuerySolution soln = results.nextSolution();
				sol.add(soln);
			}
		}catch(Exception e){
			e.printStackTrace();
			sol = null;
		}finally {
			long endTime = System.currentTimeMillis();
//		    System.out.println("Time:"+(endTime-startTime));
		    qexec.close();
		}
		_r.update(_s, sol);
	}
}
