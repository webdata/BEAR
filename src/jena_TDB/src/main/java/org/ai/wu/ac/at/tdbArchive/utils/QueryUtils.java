/**
 * 
 */
package org.ai.wu.ac.at.tdbArchive.utils;

import java.util.Iterator;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Resource;

/**
 * @author Javier Fernández
 *
 */
public final class QueryUtils {

	public static final String createLookupQuery(final String rol, String element) {
		String queryString = "SELECT ?element1 ?element2 WHERE { ";
		if (rol.equalsIgnoreCase("subject") || rol.equalsIgnoreCase("s") || rol.equalsIgnoreCase("subjects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + element + " ?element1 ?element2 .";
		} else if (rol.equalsIgnoreCase("predicate") || rol.equalsIgnoreCase("p") || rol.equalsIgnoreCase("predicates")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 " + element + " ?element2 .";
		} else if (rol.equalsIgnoreCase("object") || rol.equalsIgnoreCase("o") || rol.equalsIgnoreCase("objects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 ?element2 " + element + " .";
		}
		queryString = queryString + "}";

		return queryString;
	}

	public static final String createLookupQueryGraph(final String rol, String element) {
		String queryString = "SELECT ?element1 ?element2 ?graph WHERE { " + "GRAPH ?graph{";
		if (rol.equalsIgnoreCase("subject") || rol.equalsIgnoreCase("s") || rol.equalsIgnoreCase("subjects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + element + " ?element1 ?element2 .";
		} else if (rol.equalsIgnoreCase("predicate") || rol.equalsIgnoreCase("p") || rol.equalsIgnoreCase("predicates")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 " + element + " ?element2 .";
		} else if (rol.equalsIgnoreCase("object") || rol.equalsIgnoreCase("o") || rol.equalsIgnoreCase("objects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 ?element2 " + element + " .";
		}
		queryString = queryString + "}" + "}";

		return queryString;
	}

	public static final String createLookupQueryAnnotatedGraph(final String rol, String element, String metadataVersions) {
		String queryString = "";

		queryString = "SELECT ?element1 ?element2 ?version WHERE { " + "GRAPH <http://example.org/versions> {?graph " + metadataVersions
				+ " ?version . }\n"
				// "GRAPH <http://example.org/versions> {?graph "+metadataVersions+" ?x . }\n"
				+ "GRAPH ?graph{";

		if (rol.equalsIgnoreCase("subject") || rol.equalsIgnoreCase("s") || rol.equalsIgnoreCase("subjects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + element + " ?element1 ?element2 .";
		} else if (rol.equalsIgnoreCase("predicate") || rol.equalsIgnoreCase("p") || rol.equalsIgnoreCase("predicates")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 " + element + " ?element2 .";
		} else if (rol.equalsIgnoreCase("object") || rol.equalsIgnoreCase("o") || rol.equalsIgnoreCase("objects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 ?element2 " + element + " .";
		}

		queryString = queryString + "}" + "}";

		// System.out.println("Query is:"+queryString);
		return queryString;
	}

	public static final String createLookupQueryAnnotatedGraph(final String rol, String element, int staticVersionQuery, String metadataVersions) {
		String queryString = "";

		queryString = "SELECT ?element1 ?element2 ?graph WHERE { " + "GRAPH <http://example.org/versions> {?graph " + metadataVersions + " "
				+ staticVersionQuery + " . }\n"
				// "GRAPH <http://example.org/versions> {?graph "+metadataVersions+" ?x . }\n"
				+ "GRAPH ?graph{";

		if (rol.equalsIgnoreCase("subject") || rol.equalsIgnoreCase("s") || rol.equalsIgnoreCase("subjects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + element + " ?element1 ?element2 .";
		} else if (rol.equalsIgnoreCase("predicate") || rol.equalsIgnoreCase("p") || rol.equalsIgnoreCase("predicates")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 " + element + " ?element2 .";
		} else if (rol.equalsIgnoreCase("object") || rol.equalsIgnoreCase("o") || rol.equalsIgnoreCase("objects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 ?element2 " + element + " .";
		}

		queryString = queryString + "}" + "}";

		// System.out.println("Query is:"+queryString);
		return queryString;
	}
	public static final String createLookupQueryAnnotatedGraph(String TP, int staticVersionQuery, String metadataVersions) {
		String queryString = "";

		queryString = "SELECT ?element1 ?element2 ?graph WHERE { " + "GRAPH <http://example.org/versions> {?graph " + metadataVersions + " "
				+ staticVersionQuery + " . }\n"
				// "GRAPH <http://example.org/versions> {?graph "+metadataVersions+" ?x . }\n"
				+ "GRAPH ?graph{";
		queryString = queryString +TP;
		
		queryString = queryString + "}" + "}";

		// System.out.println("Query is:"+queryString);
		return queryString;
	}
	
	public static final String createTPLookupQuery(final String rol, String element) {
		String queryString = "";
		if (rol.equalsIgnoreCase("subject") || rol.equalsIgnoreCase("s") || rol.equalsIgnoreCase("subjects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + element + " ?element1 ?element2 .";
		} else if (rol.equalsIgnoreCase("predicate") || rol.equalsIgnoreCase("p") || rol.equalsIgnoreCase("predicates")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 " + element + " ?element2 .";
		} else if (rol.equalsIgnoreCase("object") || rol.equalsIgnoreCase("o") || rol.equalsIgnoreCase("objects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 ?element2 " + element + " .";
		}
		

		return queryString;
	}

	public static final String getGraphs() {
		String queryString = "SELECT distinct ?graph WHERE { " + "GRAPH ?graph{";

		queryString = queryString + " ?element1 ?element2 ?element3 .";

		queryString = queryString + "}" + "}";

		return queryString;
	}

	public static final String getNumGraphVersions(String metadataVersions) {
		String queryString = "SELECT (count(distinct ?element1) as ?numVersions)  WHERE { " + "GRAPH <http://example.org/versions> {?graph "
				+ metadataVersions + " ?element1 . }}";

		return queryString;
	}

	/**
	 * @param soln
	 * @param rowResult
	 * @return
	 * @return
	 */
	public static final String serializeSolution(QuerySolution soln) {
		Iterator<String> vars = soln.varNames();
		String rowResult = "";
		while (vars.hasNext()) {
			String var = vars.next();
			if (soln.get(var).isResource()) {
				Resource rs = (Resource) soln.get(var);
				rowResult += "<" + rs.getURI() + "> ";
			} else {
				rowResult += soln.getLiteral(var).asLiteral().getValue() + " ";
			}
		}
		return rowResult.trim();
	}

	/**
	 * @param soln
	 * @param rowResult
	 * @return
	 * @return
	 */
	public static final String serializeSolutionFilterOutGraphs(QuerySolution soln) {
		Iterator<String> vars = soln.varNames();
		String rowResult = "";
		while (vars.hasNext()) {
			String var = vars.next();
			if (!var.equalsIgnoreCase("graph")&& !var.equalsIgnoreCase("version")) {
				if (soln.get(var).isResource()) {
					Resource rs = (Resource) soln.get(var);
					rowResult += "<" + rs.getURI() + "> ";
				} else {
					rowResult += soln.getLiteral(var).asLiteral().getValue() + " ";
				}
			}
		}
		return rowResult.trim();
	}

}
