import java.util.Comparator;

import com.hp.hpl.jena.query.QuerySolution;


public class QuerySolutionComparator implements
		Comparator<QuerySolution> {

	@Override
	public int compare(QuerySolution o1, QuerySolution o2) {
		
		
		int d = o1.get("?1").toString().compareTo(o2.get("?1").toString());
		if(d==0){
			d = o1.get("?2").toString().compareTo(o2.get("?2").toString());
		}
		return d;
		
//		boolean s1 = ; 
//		boolean s2 =o1.get("?2").equals(o2.get("?2")) ;
//		System.out.println("compare "+o1+" with "+o2+" "+s1+" "+s2);
//		if(s1&&s2){
//			return 0;
//		}
//		else return -1;
		
	}

}
