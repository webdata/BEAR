/**
 * Interface that specifies the methods for a Jena Archive implementation
 */
package org.ai.wu.ac.at.tdbArchive.api;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.ai.wu.ac.at.tdbArchive.solutions.DiffSolution;

/**
 * @author Javier D. Fernández
 *
 */
public interface JenaTDBArchive {

	
	/**
	 * Load Jena TDB from directory 
	 * 
	 * @param directory
	 * @throws RuntimeException
	 */
	public void load (String directory) throws RuntimeException;
	
	/**
	 * close Jena TDB and release resources 
	 * 
	 * @param directory
	 * @throws RuntimeException
	 */
	public void close () throws RuntimeException;
	
	/**
	 * Gets the result of the provided query in the provided version
	 * 
	 * @param version
	 * @param queryString
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	ArrayList<String> matQuery(int version, String queryString) throws InterruptedException, ExecutionException;

	/**
	 * Reads input file with a Resource and a Version, and gets the result of a lookup of the provided Resource with the provided rol (Subject,
	 * Predicate, Object) in such Version
	 * 
	 * @param queryFile
	 * @param rol
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	ArrayList<ArrayList<String>> bulkMatQuerying(String queryFile, String rol) throws FileNotFoundException, IOException, InterruptedException, ExecutionException;

	/**
	 * Reads input file with a Resource, and gets the result of a lookup of the provided Resource with the provided rol (Subject,
	 * Predicate, Object) for every version
	 * 
	 * @param queryFile
	 * @param rol
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	ArrayList<Map<Integer,ArrayList<String>>> bulkAllMatQuerying(String queryFile, String rol) throws FileNotFoundException, IOException, InterruptedException, ExecutionException;

	/**
	 * Gets the diff of the provided query between the two given versions
	 * 
	 * @param startVersionQuery
	 * @param endVersionQuery
	 * @param queryString
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	DiffSolution diffQuerying(int startVersionQuery, int endVersionQuery, String queryString) throws InterruptedException,
			ExecutionException;

	/**
	 * Reads input file with a Resource, and gets the diff result of the lookup of the provided Resource with the provided rol (Subject,
	 * Predicate, Object) for all versions between 0 and consecutive jumps
	 * 
	 * @param queryFile
	 * @param rol
	 * @param jump
	 * @return the add and delete solutions for each version
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	ArrayList<Map<Integer,DiffSolution>> bulkAlldiffQuerying(String queryFile, String rol, int jump) throws InterruptedException, ExecutionException, IOException;

	/**
	 * Get the results of the provided query in all versions
	 * 
	 * @param queryString
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	Map<Integer,ArrayList<String>> verQuery( String queryString) throws InterruptedException, ExecutionException;

	/**
	 * Reads input file with a Resource, and gets all result of the lookup of the provided Resource with the provided rol (Subject,
	 * Predicate, Object) for all versions 
	 * 
	 * @param queryFile
	 * @param rol
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	ArrayList<Map<Integer,ArrayList<String>>> bulkAllVerQuerying(String queryFile, String rol) throws InterruptedException, ExecutionException, IOException;

	
	/**
	 * Reads input file with a Resource, and gets all the consecutive versions where the query is different
	 * 
	 * @param queryFile
	 * @param rol
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	ArrayList<ArrayList<Integer>> bulkAllChangeQuerying(String queryFile, String rol) throws InterruptedException, ExecutionException, IOException;
	/**
	 * Warmup the system 
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	void warmup() throws InterruptedException, ExecutionException;
	
	/**
	 * @param outputTime
	 */
	public void setOutputTime(String outputTime);

	/**
	 * Reads input file with a Resource, and gets the result of a join of the provided Resource with the provided roles (Subject, Predicate, Object)
	 * @param queryFile
	 * @param rol1
	 * @param rol2
	 * @param join
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public ArrayList<Map<Integer, ArrayList<String>>> bulkAllJoinQuerying(String queryFile, String rol1, String rol2, String join) throws FileNotFoundException, IOException,
			InterruptedException, ExecutionException;
	
}
