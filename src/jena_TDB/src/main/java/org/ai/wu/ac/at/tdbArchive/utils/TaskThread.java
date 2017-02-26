package org.ai.wu.ac.at.tdbArchive.utils;

import java.util.TreeMap;


import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecutionFactory;

/** Try to ping a URL. Return true only if successful. */
public final class TaskThread extends Thread {

	TreeMap<Integer, QueryResult> res;

	public TaskThread(Query query, Dataset dataset, int version, TreeMap<Integer, QueryResult> res) {
		this.query = query;
		this.dataset = dataset;
		this.version = version;
		this.res = res;
		this.isAdd = true;
	}
	public TaskThread(Query query, Dataset dataset, int version, Boolean isAdd, TreeMap<Integer, QueryResult> res) {
		this.query = query;
		this.dataset = dataset;
		this.version = version;
		this.res = res;
		this.isAdd = isAdd;
	}

	@Override
	public synchronized void start() {
		// System.out.println("version:"+this.version);
		// System.out.println("query:"+this.query);
		// System.out.println("isAdd:"+this.isAdd);
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
	private final Boolean isAdd;
}