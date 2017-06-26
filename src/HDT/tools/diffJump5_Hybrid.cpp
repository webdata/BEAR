#include <iostream>
#include <HDTManager.hpp>
#include <stdio.h>
#include <stdlib.h>     /* atoi */
#include <sstream>
#include <getopt.h>
#include <iostream>
#include <fstream>
#include "../src/util/StopWatch.hpp"
#include "../src/hdt/BasicHDT.hpp"
#include <HDT.hpp>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;
using namespace hdt;

// VERSIONS START WITH 0
map<int, HDT*> HDTversions; // Version IC-> HDT
map<int, HDT*> HDTversions_add; // Version diff add-> HDT
map<int, HDT*> HDTversions_del; // Version diff del-> HDT
set<int> materializedVersions; //includes the versions with IC

void help() {
	cout << "$ diffJump5_Hybrid [options] <hdtfile> " << endl;
	cout << "\t-h\t\t\tThis help" << endl;
	cout << "\t-d\t<dir>\t\tGet HDTs from the given directory." << endl;
	cout << "\t-i\t<query>\t\tLaunch query and exit." << endl;
	//cout << "\t-o\t<output>\tSave query output to file." << endl;
	cout << "\t-t\t<type>\t\ttype of query [s, p, o]." << endl;
	cout << "\t-o\t<output>\tSave query output to file." << endl;
	cout << "\t-l\t<number>\t\tlimit upt to <number> of versions" << endl;
	//cout << "\t-v\tVerbose output" << endl;
}
vector<string> split(const string& str, const string& delim) {
	vector<string> tokens;
	size_t prev = 0, pos = 0;
	do {
		pos = str.find(delim, prev);
		if (pos == string::npos)
			pos = str.length();
		string token = str.substr(prev, pos - prev);
		if (!token.empty())
			tokens.push_back(token);
		prev = pos + delim.length();
	} while (pos < str.length() && prev < str.length());
	return tokens;
}
inline bool exists_File(const std::string& name) {
	struct stat buffer;
	return (stat(name.c_str(), &buffer) == 0);
}

void warm(HDT* hdt) {
	IteratorTripleString *it = hdt->search("", "", "");
	int count = 0;
	while (it->hasNext() && count < 100) {
		TripleString *triple = it->next();
		/*cout << "Result Warmup: " << triple->getSubject() << ", "
		 << triple->getPredicate() << ", " << triple->getObject()
		 << endl;
		 */
		count++;
	}
	delete it; // Remember to delete iterator to avoid memory leaks!
}

int main(int argc, char *argv[]) {

	int c;
	string inputFile, outputFile, limit;
	string directory = "data/hybridHDT/";
	string type = "null";
	while ((c = getopt(argc, argv, "hi:t:l:o:d:")) != -1) {
		switch (c) {
		case 'h':
			help();
			break;
		case 'i':
			inputFile = optarg;
			break;
		case 'o':
			outputFile = optarg;
			break;
		case 't':
			type = optarg;
			break;
		case 'l':
			limit = optarg;
			break;
		case 'd':
			directory = optarg;
			break;
		default:
			cout << "ERROR: Unknown option" << endl;
			help();
			return 1;
		}
	}
	// Load HDT file

	int numVersions = 0;
	if (limit.length() > 0) {
		numVersions = atoi((char*) limit.c_str());
	} else {
		cerr << "[WARNING] limit not provided, trying to load 58 versions"
				<< endl;
		numVersions = 58;
	}
	ostream *out;
	ofstream outF;

	if (outputFile != "") {
		outF.open(outputFile.c_str());
		out = &outF;
	} else {
		out = &cout;
	}

	if (directory[directory.length() - 1] != '/') {
		directory += "/";
	}

	for (int i = 0; i < numVersions; i++) {
		std::stringstream sstm;

		// check IC file
		sstm << directory << (i + 1) << ".hdt";
		string fileString = sstm.str();
		if (exists_File(fileString)) {
			cout << "Loading " << sstm.str() << endl;
			HDTversions[i] = HDTManager::mapIndexedHDT(
					(char*) sstm.str().c_str());
			materializedVersions.insert(i);
		}
		// check adds
		sstm.str("");
		sstm.clear();
		sstm << directory << (i + 1) << ".add.hdt";
		fileString = sstm.str();
		//cout << "fileString is: " << fileString << endl;
		if (exists_File(fileString)) {
			HDTversions_add[i] = HDTManager::mapIndexedHDT(
					(char*) sstm.str().c_str());
			cout << "Loading " << fileString << endl;
		}

		//check deletes
		sstm.str("");
		sstm.clear();
		sstm << directory << (i + 1) << ".del.hdt";
		fileString = sstm.str();
		//cout << "fileString is: " << fileString << endl;

		if (exists_File(fileString)) {
			HDTversions_del[i] = HDTManager::mapIndexedHDT(
					(char*) sstm.str().c_str());
			cout << "Loading " << fileString << endl;
		}

	}

	cout << "WARMUP... " << endl;
	for (int i = 0; i < numVersions; i++) {
		// Warmup all sets
		if (HDTversions[i] != NULL) {
			cout << "HDTversions " << i << endl;
			warm(HDTversions[i]);
		}
		if (HDTversions_add[i] != NULL) {
			cout << "HDTversions add " << i << endl;
			warm(HDTversions_add[i]);
		}
		if (HDTversions_del[i] != NULL) {
			cout << "HDTversions del " << i << endl;
			warm(HDTversions_del[i]);
		}
	}

	cout << "... WARMUP finished!" << endl;

	if (type == "null") {
		cerr << "[ERROR] Please provide a type of query (-t [s,p,o])" << endl;
		help();
		exit(0);
	}

	//read queries
	cout << "opening file:" << inputFile << endl;
	std::ifstream file((char*) inputFile.c_str());
	//cout << "opened! " << endl;

	if (!file.good())
		throw "unable to open filter file";
	string linea = "";

	vector<double> times(numVersions, 0);
	int num_queries = 0;
	while (!file.eof()) {
		getline(file, linea);
		//cout << "Reading line:" << linea << endl;

		if (linea.length() == 0)
			continue;
		size_t pos = linea.find(' ');

		if (pos != std::string::npos) {
			string query = linea.substr(0, pos);
			string subject = "", predicate = "", object = "";
			if (type == "s") {
				subject = query;
			} else if (type == "p") {
				predicate = query;
			} else if (type == "o") {
				object = query;
			} else {
				vector<string> elements = split(linea, " ");
				if (type == "sp") {
					subject = elements[0];
					predicate = elements[1];
				} else if (type == "so") {
					subject = elements[0];
					object = elements[1];
				} else if (type == "po") {
					predicate = elements[0];
					object = elements[1];
				} else if (type == "spo") {
					subject = elements[0];
					predicate = elements[1];
					object = elements[2];
				}
			}

			int jump = 5;
			int totalIterations = ((numVersions - 1) / jump) + 1; //-1 because we start in 0
			for (int i = 0; i < totalIterations; i++) {
				int versionQuery = 0; //always compare against the first version=0
				int postversionQuery = min((i + 1) * jump, numVersions) - 1;
				//cout << "diff between " << versionQuery << " "<< postversionQuery << endl;

				//this should be in parallel but we do it sequential because is very fast so far
				StopWatch st;

				set<string> results_add;
				set<string> results_del;
				int numResults = 0;

				// We assume we compare against the first version, otherwise we have to search for lower bound
				IteratorTripleString *it1 = HDTversions[versionQuery]->search(
						subject.c_str(), predicate.c_str(), object.c_str());
				set<string> results1;
				while (it1->hasNext()) {
					TripleString *triple1 = it1->next();
					results1.insert(
							triple1->getSubject() + " "
									+ triple1->getPredicate() + " "
									+ triple1->getObject());
				}

				set<string> results2;
				// first check if postversionQuery is in IC
				if (HDTversions[postversionQuery] != NULL) {
					//cout << "staticVersionQuery is in IC" << endl;
					IteratorTripleString *it =
							HDTversions[postversionQuery]->search(
									subject.c_str(), predicate.c_str(),
									object.c_str());
					while (it->hasNext()) {
						TripleString *triple = it->next();
						results2.insert(
								triple->getSubject() + " "
										+ triple->getPredicate() + " "
										+ triple->getObject());
					}
					delete it;
				} else {
					//get closest IC less than the postversionQuery version
					std::set<int>::iterator lowerIC =
							materializedVersions.lower_bound(postversionQuery);
					if (lowerIC == materializedVersions.end()) { //if all ICs are less than the current version, then go to the last one
						lowerIC--;
					}
					if ((*lowerIC) > postversionQuery) { //lower_bound gives where staticVersionQuery is found, or the element after if it is not found
						lowerIC--;
					}
					int closestIC = (*lowerIC);

					//cout << "Not in IC. Closest closestIC is: " << closestIC << endl;
					// get first results from IC
					// no need to search if closestIC = versioNQuery (the inital version)
					if (closestIC == versionQuery) {
						set<string> copyresult1(results1.begin(),
								results1.end());
						results2 = copyresult1;
						//cout << "no need to search if closestIC = versioNQuery"<< endl;
					} else {
						IteratorTripleString *it =
								HDTversions[closestIC]->search(subject.c_str(),
										predicate.c_str(), object.c_str());
						while (it->hasNext()) {
							TripleString* triple = it->next();
							//cout << "Result: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
							results2.insert(
									triple->getSubject() + " "
											+ triple->getPredicate() + " "
											+ triple->getObject());
						}
					}
					// Now retrieve the Deltas
					for (int i = closestIC + 1; i <= postversionQuery; i++) {
						//cout << "search add in i=" << i << endl;
						IteratorTripleString* it_add =
								HDTversions_add[i]->search(subject.c_str(),
										predicate.c_str(), object.c_str());

						while (it_add->hasNext()) {
							TripleString* triple = it_add->next();
							//cout << "Result: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
							results2.insert(
									triple->getSubject() + " "
											+ triple->getPredicate() + " "
											+ triple->getObject());

						}
						//cout << numResults << " results inserted" << endl;
						delete it_add;
						//cout << "search del in i=" << i << endl;

						IteratorTripleString* it_del =
								HDTversions_del[i]->search(subject.c_str(),
										predicate.c_str(), object.c_str());

						while (it_del->hasNext()) {
							TripleString* tripledel = it_del->next();
							//cout << "Result: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
							results2.erase(
									tripledel->getSubject() + " "
											+ tripledel->getPredicate() + " "
											+ tripledel->getObject());

						}
						delete it_del;

						//cout << numResults << " results after deletion" << endl;
					}
				}

				// compute the deltas between result1 (initial) and results2 (end)
				numResults = 0;
				int adds = 0;
				int dels = 0;
				//cout<<"results1 size:"<<results1.size()<<endl;
				//cout<<"results2 size:"<<results2.size()<<endl;
				for (std::set<string>::iterator it = results1.begin();
						it != results1.end(); ++it) {
					if (results2.find(*it) == results2.end()) {
						dels++;
						//cout << "del: " << *it << endl;
					}
				}
				for (std::set<string>::iterator it = results2.begin();
						it != results2.end(); ++it) {
					if (results1.find(*it) == results1.end()) {
						adds++;
						//cout << "add: " << *it << endl;
					}
				}

				double time = st.toMillis();
				times[i] = times[i] + time;
				//cout << "-- " << adds << " adds and " << dels << " dels, in "<< time << " ms" << endl;

			}
			num_queries++;
		}
	}
	cout << "compute mean of queries" << endl;
	fflush(stdout);
	//compute mean of queries
	*out << "<version>,<mean_time>,<total>" << endl;
	for (int i = 0; i < numVersions; i++) {
		*out << (i + 1) << "," << times[i] / num_queries <<","<<times[i] << endl;
	}

	for (int i = 0; i < numVersions; i++) {
		delete HDTversions_add[i]; // Remember to delete instance when no longer needed!
		delete HDTversions_del[i]; // Remember to delete instance when no longer needed!
		delete HDTversions[i]; // Remember to delete instance when no longer needed!
	}
	if (outputFile != "") {
		outF.close();
	}

}
