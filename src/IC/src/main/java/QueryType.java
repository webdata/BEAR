import java.util.Map;
import java.util.TreeSet;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;


public class QueryType {

	
	
	public static TreeSet<QuerySolution> execute(String query, Dataset d){
		Query q = QueryFactory.create(query);
		QueryExecution qexec = QueryExecutionFactory.create(q, d);
		TreeSet<QuerySolution> sol = new TreeSet<QuerySolution>(new QuerySolutionComparator());
		try {
			
			ResultSet results = qexec.execSelect();
			while (results.hasNext()) {
				QuerySolution soln = results.nextSolution();
				sol.add(soln);

			}
		}catch(Exception e){
			sol = null;
		}finally {
			qexec.close();
		}
		return sol;
	}
}
