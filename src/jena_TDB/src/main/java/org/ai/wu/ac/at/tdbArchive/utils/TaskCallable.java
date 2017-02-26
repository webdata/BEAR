package org.ai.wu.ac.at.tdbArchive.utils;

import java.util.concurrent.Callable;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecutionFactory;

/** Try to ping a URL. Return true only if successful. */
public final class TaskCallable implements Callable<QueryResult> {

	public TaskCallable(Query query, Dataset dataset, int version, Boolean isAdd) {
		this.query = query;
		this.dataset = dataset;
		this.version = version;
		this.isAdd = isAdd;
		
	}

	/** Access a URL, and see if you get a healthy response. */
	public QueryResult call() throws Exception {
		QueryResult ret = new QueryResult();
		ret.setEx(QueryExecutionFactory.create(query, dataset));
		ret.setSol(ret.getEx().execSelect());
		ret.setVersion(version);
		ret.setIsAdd(isAdd);
		return ret;
	}

	public Boolean getIsAdd() {
		return isAdd;
	}

	private final Query query;
	private final Dataset dataset;
	private final int version;
	private final Boolean isAdd; // otherwise is del
	
}