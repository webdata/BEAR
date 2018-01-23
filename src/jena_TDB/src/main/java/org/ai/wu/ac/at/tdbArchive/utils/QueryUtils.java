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

	public static final QueryRol getQueryRol(String rol){
		if (rol.equalsIgnoreCase("subject") || rol.equalsIgnoreCase("s") || rol.equalsIgnoreCase("subjects")) {
			return QueryRol.S;
		}
		else if (rol.equalsIgnoreCase("predicate") || rol.equalsIgnoreCase("p") || rol.equalsIgnoreCase("predicates")) {
			return QueryRol.P;
		}
		else if (rol.equalsIgnoreCase("object") || rol.equalsIgnoreCase("o") || rol.equalsIgnoreCase("objects")) {
			return QueryRol.O;
		}
		else if (rol.equalsIgnoreCase("SP") || rol.equalsIgnoreCase("subjectpredicate")){
			return QueryRol.SP;
		}
		else if (rol.equalsIgnoreCase("SO") || rol.equalsIgnoreCase("subjectobject")){
			return QueryRol.SO;
		}
		else if (rol.equalsIgnoreCase("PO") || rol.equalsIgnoreCase("predicateobject")){
			return QueryRol.PO;
		}
		else if (rol.equalsIgnoreCase("SPO") || rol.equalsIgnoreCase("subjectpredicateobject")){
			return QueryRol.SPO;
		}
		else return QueryRol.ALL;
	}

	public static final String createLookupQuery(String queryType, String term) {
		String[] terms={term};
		return createLookupQuery(queryType,terms);
	}
	public static final String createLookupQuery(String queryType, String[] terms) {
		QueryRol qtype = getQueryRol(queryType);
		String subject, predicate, object;
		String queryString="";
		if (qtype==QueryRol.S){
			subject = terms[0];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			queryString = "SELECT ?element1 ?element2 WHERE { "+ subject +" ?element1 ?element2 . }";
		}
		else if (qtype==QueryRol.P){
			predicate = terms[0];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?element2 WHERE { ?element1 "+ predicate+" ?element2 . }";
		}
		else if (qtype==QueryRol.O){
			object = terms[0];
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?element2 WHERE { ?element1 ?element2 "+ object+" . }";
		}
		else if (qtype==QueryRol.SP){
			subject = terms[0];
			predicate = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 WHERE { "+ subject +" "+predicate+" ?element1 . }";
		}
		else if (qtype==QueryRol.SO){
			subject = terms[0];
			object = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 WHERE { "+ subject +" ?element1 "+object+" . }";
		}
		else if (qtype==QueryRol.PO){
			predicate = terms[0];
			object = terms[1];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 WHERE { ?element1 "+predicate+ " " +object+" . }";
		}
		else if (qtype==QueryRol.SPO){
			subject = terms[0];
			predicate = terms[1];
			object = terms[2];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "ASK WHERE { "+subject+" "+predicate+ " " +object+" . }";
		}
		else{ //if (qtype==QueryRol.ALL){
		
			queryString = "SELECT * WHERE { ?element1 ?element2 ?element3 . }";
		}
		

		return queryString;
	}

	public static final String createLookupQueryGraph(String queryType, String term) {
		String[] terms={term};
		return createLookupQueryGraph(queryType,terms);
	}
	
	public static final String createLookupQueryGraph(final String queryType, String[] terms) {
		QueryRol qtype = getQueryRol(queryType);
		String subject, predicate, object;
		String queryString="";
			
		if (qtype==QueryRol.S){
			subject = terms[0];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			queryString = "SELECT ?element1 ?element2 ?graph WHERE { GRAPH ?graph{ "+ subject +" ?element1 ?element2 . } }";
		}
		else if (qtype==QueryRol.P){
			predicate = terms[0];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?element2 ?graph WHERE { GRAPH ?graph{ ?element1 "+ predicate +" ?element2 . } }";
		}
		else if (qtype==QueryRol.O){
			object = terms[0];
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?element2 ?graph WHERE { GRAPH ?graph{ ?element1 ?element2 "+ object +" . } }";
		}
		else if (qtype==QueryRol.SP){
			subject = terms[0];
			predicate = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?graph WHERE { GRAPH ?graph{ "+ subject +" "+predicate+" ?element1 . }}";
		}
		else if (qtype==QueryRol.SO){
			subject = terms[0];
			object = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?graph WHERE { GRAPH ?graph{ "+ subject +" ?element1 "+object+" . }}";
		}
		else if (qtype==QueryRol.PO){
			predicate = terms[0];
			object = terms[1];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?graph WHERE { GRAPH ?graph{ ?element1 "+predicate+ " " +object+" . }}";
		}
		else if (qtype==QueryRol.SPO){
			subject = terms[0];
			predicate = terms[1];
			object = terms[2];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?graph WHERE { GRAPH ?graph{"+subject+" "+predicate+ " " +object+" . }}";
		}
		else{ //if (qtype==QueryRol.ALL){
		
			queryString = "SELECT * WHERE { GRAPH ?graph{ ?element1 ?element2 ?element3 . }}";
		}
		return queryString;
	}

	public static final String createLookupQueryAnnotatedGraph(String queryType, String term, String metadataVersions) {
		String[] terms={term};
		return createLookupQueryAnnotatedGraph(queryType,terms,metadataVersions);
	}
	
	public static final String createLookupQueryAnnotatedGraph(String queryType, String[] terms, String metadataVersions) {
		String queryString = "";
		QueryRol qtype = getQueryRol(queryType);
		String subject, predicate, object;
		
		String graphWHERE= "GRAPH <http://example.org/versions> {?graph " + metadataVersions + " ?version . }\n";
		
		if (qtype==QueryRol.S){
			subject = terms[0];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			queryString = "SELECT ?element1 ?element2 ?version WHERE { "+ graphWHERE + "GRAPH ?graph{ "+subject+" ?element1 ?element2 ."+"} }";
		}
		else if (qtype==QueryRol.P){
			predicate = terms[0];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?element2 ?version WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 "+predicate+" ?element2 ."+"} }";
		}
		else if (qtype==QueryRol.O){
			object = terms[0];
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?element2 ?version WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 ?element2 "+object+"."+"} }";
		}
		else if (qtype==QueryRol.SP){
			subject = terms[0];
			predicate = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?version WHERE { "+ graphWHERE + "GRAPH ?graph{ "+subject+" "+predicate+" ?element1 ."+"} }";
		}
		else if (qtype==QueryRol.SO){
			subject = terms[0];
			object = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?version WHERE { "+ graphWHERE + "GRAPH ?graph{ "+subject+" ?element1 "+object+" ."+"} }";
		}
		else if (qtype==QueryRol.PO){
			predicate = terms[0];
			object = terms[1];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?version WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 "+predicate+" "+object+" ."+"} }";
		}
		else if (qtype==QueryRol.SPO){
			subject = terms[0];
			predicate = terms[1];
			object = terms[2];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?version WHERE { "+graphWHERE + "GRAPH ?graph{ "+subject+" "+predicate+" "+object+" ."+"} }";
		}
		else{ //if (qtype==QueryRol.ALL){
		
			queryString = "SELECT * WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 ?element2 ?element3 . } }";
		}
		

		return queryString;
	}
	/*
	public static final String createLookupQueryAnnotatedGraph2(final String rol, String element, String metadataVersions) {
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
	}*/

	public static final String createLookupQueryAnnotatedGraph(final String queryType, String term, int staticVersionQuery, String metadataVersions) {
		String[] terms={term};
		return createLookupQueryAnnotatedGraph(queryType,terms,staticVersionQuery,metadataVersions);
	}
	
	public static final String createLookupQueryAnnotatedGraph(final String queryType, String[] terms, int staticVersionQuery, String metadataVersions) {
		String queryString = "";
		QueryRol qtype = getQueryRol(queryType);
		String subject, predicate, object;
		
		String graphWHERE= "GRAPH <http://example.org/versions> {?graph " + metadataVersions + " "
				+ staticVersionQuery + " . }\n";
		
		if (qtype==QueryRol.S){
			subject = terms[0];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			queryString = "SELECT ?element1 ?element2 ?graph WHERE { "+ graphWHERE + "GRAPH ?graph{ "+subject+" ?element1 ?element2 ."+"} }";
		}
		else if (qtype==QueryRol.P){
			predicate = terms[0];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?element2 ?graph WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 "+predicate+" ?element2 ."+"} }";
		}
		else if (qtype==QueryRol.O){
			object = terms[0];
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?element2 ?graph WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 ?element2 "+object+"."+"} }";
		}
		else if (qtype==QueryRol.SP){
			subject = terms[0];
			predicate = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?graph WHERE { "+ graphWHERE + "GRAPH ?graph{ "+subject+" "+predicate+" ?element1 ."+"} }";
		}
		else if (qtype==QueryRol.SO){
			subject = terms[0];
			object = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?graph WHERE { "+ graphWHERE + "GRAPH ?graph{ "+subject+" ?element1 "+object+" ."+"} }";
		}
		else if (qtype==QueryRol.PO){
			predicate = terms[0];
			object = terms[1];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?graph WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 "+predicate+" "+object+" ."+"} }";
		}
		else if (qtype==QueryRol.SPO){
			subject = terms[0];
			predicate = terms[1];
			object = terms[2];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "ASK WHERE { "+graphWHERE + "GRAPH ?graph{ "+subject+" "+predicate+" "+object+" ."+"} }";
		}
		else{ //if (qtype==QueryRol.ALL){
		
			queryString = "SELECT * WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 ?element2 ?element3 . } }";
		}
		

		return queryString;
	}
	/*
	public static final String createLookupQueryAnnotatedGraph2(final String rol, String element, int staticVersionQuery, String metadataVersions) {
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

		 //System.out.println("Query is:"+queryString);
		return queryString;
	}*/
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
	
	public static final String getTP(final String rol, String[] terms) {
		String queryString = "";
		String subject, predicate, object;
		QueryRol qtype = getQueryRol(rol);
		if (qtype==QueryRol.S){
			subject = terms[0];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			queryString = subject +" ?element1 ?element2 ";
		}
		else if (qtype==QueryRol.P){
			predicate = terms[0];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?element2 WHERE { ?element1 "+ predicate+" ?element2 . }";
		}
		else if (qtype==QueryRol.O){
			object = terms[0];
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?element2 WHERE { ?element1 ?element2 "+ object+" . }";
		}
		else if (qtype==QueryRol.SP){
			subject = terms[0];
			predicate = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 WHERE { "+ subject +" "+predicate+" ?element1 . }";
		}
		else if (qtype==QueryRol.SO){
			subject = terms[0];
			object = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 WHERE { "+ subject +" ?element1 "+object+" . }";
		}
		else if (qtype==QueryRol.PO){
			predicate = terms[0];
			object = terms[1];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 WHERE { ?element1 "+predicate+ " " +object+" . }";
		}
		else if (qtype==QueryRol.SPO){
			subject = terms[0];
			predicate = terms[1];
			object = terms[2];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "ASK WHERE { "+subject+" "+predicate+ " " +object+" . }";
		}
		else{ //if (qtype==QueryRol.ALL){
		
			queryString = "SELECT * WHERE { ?element1 ?element2 ?element3 . }";
		}
		

		return queryString;
	}
	
	public static final String createLookupQueryfirsTP(String queryType1,String[] terms) {
		QueryRol qtype1 = getQueryRol(queryType1);
		String subject1="?element1", predicate1="?element2", object1="?element3";
		String queryString="";
		
		if ((qtype1==QueryRol.S)||(qtype1==QueryRol.SP)||(qtype1==QueryRol.SO)||(qtype1==QueryRol.SPO)){
			subject1 = terms[0];
		
			if (subject1.startsWith("http"))
				subject1 = "<" + subject1 + ">";
		}
		if (qtype1==QueryRol.P||qtype1==QueryRol.PO){
			predicate1 = terms[0];
		
			if (predicate1.startsWith("http"))
				predicate1 = "<" + predicate1 + ">";
		}
		if (qtype1==QueryRol.SP || qtype1==QueryRol.SPO ){
			predicate1 = terms[1];
		
			if (predicate1.startsWith("http"))
				predicate1 = "<" + predicate1 + ">";
		}
		if (qtype1==QueryRol.O){
			object1 = terms[0];
		
			if (object1.startsWith("http"))
				object1 = "<" + object1 + ">";
		}
		if (qtype1==QueryRol.SO||qtype1==QueryRol.PO){
			object1 = terms[1];
		
			if (object1.startsWith("http"))
				object1 = "<" + object1 + ">";
		}
		if (qtype1==QueryRol.SPO){
			object1 = terms[2];
		
			if (object1.startsWith("http"))
				object1 = "<" + object1 + ">";
		}
		
		
		queryString = "SELECT * WHERE { "+subject1+" "+predicate1+" "+object1+" . }";
		
				

		return queryString;
	}

	public static final String createJoinQueryFromIntermediate(String queryType1,String queryType2,String join,String intermediateSol, String[] terms) {
		QueryRol qtype1 = getQueryRol(queryType1);
		QueryRol qtype2 = getQueryRol(queryType2);
		String subject2="?element4", predicate2="?element5", object2="?element6";
		String queryString="";
		int numTerms=0;
		if ((qtype1==QueryRol.S)||(qtype1==QueryRol.P)||(qtype1==QueryRol.O)){
			numTerms=1;
		}
		
		if ((qtype1==QueryRol.SP)||(qtype1==QueryRol.SO)||(qtype1==QueryRol.PO)){
			numTerms=2;
		}
		
		if (qtype1==QueryRol.SPO ){
			numTerms=3;
		}
		// split the intermediate result
		String[] parts = intermediateSol.split(" ");
/////
		if ((qtype2==QueryRol.S)||(qtype2==QueryRol.SP)||(qtype2==QueryRol.SO)||(qtype2==QueryRol.SPO)){
			subject2 = terms[numTerms];
			numTerms++;
			if (subject2.startsWith("http"))
				subject2 = "<" + subject2 + ">";
		}
		if (qtype2==QueryRol.P||qtype2==QueryRol.PO){
			predicate2 = terms[numTerms];
			numTerms++;
			if (predicate2.startsWith("http"))
				predicate2 = "<" + predicate2 + ">";
		}
		if (qtype2==QueryRol.SP || qtype2==QueryRol.SPO ){
			predicate2 = terms[numTerms];
			numTerms++;
			if (predicate2.startsWith("http"))
				predicate2 = "<" + predicate2 + ">";
		}
		if (qtype2==QueryRol.O){
			object2 = terms[numTerms];
			numTerms++;
			if (object2.startsWith("http"))
				object2 = "<" + object2 + ">";
		}
		if (qtype2==QueryRol.SO||qtype2==QueryRol.PO){
			object2 = terms[numTerms];
			numTerms++;
			if (object2.startsWith("http"))
				object2 = "<" + object2 + ">";
		}
		if (qtype2==QueryRol.SPO){
			object2 = terms[numTerms];
			numTerms++;
			if (object2.startsWith("http"))
				object2 = "<" + object2 + ">";
		}
		System.out.println("join is:"+join);
		
		Boolean askQuery=false;
		if (join.equalsIgnoreCase("ss")){
			subject2 = parts[0]; //first component of the solution in any case
			if (!object2.startsWith("?")&& !predicate2.startsWith("?")){
				askQuery=true;
			}
			
		}
		else if (join.equalsIgnoreCase("oo")){
			if ((qtype1==QueryRol.S)||(qtype1==QueryRol.P)){
				object2 = parts[1]; //second component of the solution
				
			}
			else if ((qtype1==QueryRol.SP)){
				object2 = parts[0]; //second component of the solution
			}
			else if ((qtype1==QueryRol.ALL)){
				object2 = parts[2]; //second component of the solution
			}
			if (!subject2.startsWith("?")&& !predicate2.startsWith("?")){
				askQuery=true;
			}
		}
		else if (join.equalsIgnoreCase("so")){
			if ((qtype1==QueryRol.P)||(qtype1==QueryRol.S)){
				subject2 = parts[1]; //second component of the solution
			}
			else if ((qtype1==QueryRol.SP)){
				subject2 = parts[0]; //first component of the solution
			}
			else if ((qtype1==QueryRol.ALL)){
				subject2 = parts[2]; //second component of the solution
			}
			if (!object2.startsWith("?")&& !predicate2.startsWith("?")){
				askQuery=true;
			}
		}
		
		
		if (!askQuery)
			queryString = "SELECT * WHERE { "+subject2+" "+predicate2+" "+object2+" .}";
		else
			queryString = "ASK WHERE { "+subject2+" "+predicate2+ " " +object2+" . }";
		
		return queryString;
	}
	public static final String createJoinQuery(String queryType1,String queryType2, String join,String[] terms) {
		QueryRol qtype1 = getQueryRol(queryType1);
		String subject1="?element1", predicate1="?element2", object1="?element3";
		QueryRol qtype2 = getQueryRol(queryType2);
		String subject2="?element4", predicate2="?element5", object2="?element6";
		String queryString="";
		int numTerms=0;
		if ((qtype1==QueryRol.S)||(qtype1==QueryRol.SP)||(qtype1==QueryRol.SO)||(qtype1==QueryRol.SPO)){
			subject1 = terms[0];
			numTerms++;
			if (subject1.startsWith("http"))
				subject1 = "<" + subject1 + ">";
		}
		if (qtype1==QueryRol.P||qtype1==QueryRol.PO){
			predicate1 = terms[0];
			numTerms++;
			if (predicate1.startsWith("http"))
				predicate1 = "<" + predicate1 + ">";
		}
		if (qtype1==QueryRol.SP || qtype1==QueryRol.SPO ){
			predicate1 = terms[1];
			numTerms++;
			if (predicate1.startsWith("http"))
				predicate1 = "<" + predicate1 + ">";
		}
		if (qtype1==QueryRol.O){
			object1 = terms[0];
			numTerms++;
			if (object1.startsWith("http"))
				object1 = "<" + object1 + ">";
		}
		if (qtype1==QueryRol.SO||qtype1==QueryRol.PO){
			object1 = terms[1];
			numTerms++;
			if (object1.startsWith("http"))
				object1 = "<" + object1 + ">";
		}
		if (qtype1==QueryRol.SPO){
			object1 = terms[2];
			numTerms++;
			if (object1.startsWith("http"))
				object1 = "<" + object1 + ">";
		}
		
/////
		if ((qtype2==QueryRol.S)||(qtype2==QueryRol.SP)||(qtype2==QueryRol.SO)||(qtype2==QueryRol.SPO)){
			subject2 = terms[numTerms];
			numTerms++;
			if (subject2.startsWith("http"))
				subject2 = "<" + subject2 + ">";
		}
		if (qtype2==QueryRol.P||qtype2==QueryRol.PO){
			predicate2 = terms[numTerms];
			numTerms++;
			if (predicate2.startsWith("http"))
				predicate2 = "<" + predicate2 + ">";
		}
		if (qtype2==QueryRol.SP || qtype2==QueryRol.SPO ){
			predicate2 = terms[numTerms];
			numTerms++;
			if (predicate2.startsWith("http"))
				predicate2 = "<" + predicate2 + ">";
		}
		if (qtype2==QueryRol.O){
			object2 = terms[numTerms];
			numTerms++;
			if (object2.startsWith("http"))
				object2 = "<" + object2 + ">";
		}
		if (qtype2==QueryRol.SO||qtype2==QueryRol.PO){
			object2 = terms[numTerms];
			numTerms++;
			if (object2.startsWith("http"))
				object2 = "<" + object2 + ">";
		}
		if (qtype2==QueryRol.SPO){
			object2 = terms[numTerms];
			numTerms++;
			if (object2.startsWith("http"))
				object2 = "<" + object2 + ">";
		}
		
		if (join.equalsIgnoreCase("ss")){
			subject2 = subject1;
		}
		else if (join.equalsIgnoreCase("oo")){
			object2 = object1;
		}
		else if (join.equalsIgnoreCase("so")){
			subject2 = object1;
		}
		
		
		queryString = "SELECT * WHERE { "+subject1+" "+predicate1+" "+object1+" . "+ subject2+" "+predicate2+" "+object2+" .}";
		
				

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
				//rowResult += soln.getLiteral(var).asLiteral().getValue() + " ";
				rowResult += "\""+soln.getLiteral(var).getString()+"\"";
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
					//rowResult += soln.getLiteral(var).asLiteral().getValue() + " ";
					rowResult += soln.getLiteral(var).getString();
				}
			}
		}
		return rowResult.trim();
	}

}
