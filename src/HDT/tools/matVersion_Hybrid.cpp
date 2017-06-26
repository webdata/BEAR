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
	cout << "$ matVersion_Hybrid [options] <hdtfile> " << endl;
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

inline int materializeQuery(int staticVersionQuery, const string& subject,
		const string& predicate, const string& object) {
	set<string> results;
	//this should be in parallel but we do it sequential because is very fast so far

	int numResults = 0;
	// first check if staticVersionQuery is in IC
	if (HDTversions[staticVersionQuery] != NULL) {
		//cout << "staticVersionQuery is in IC" << endl;
		IteratorTripleString *it = HDTversions[staticVersionQuery]->search(
				subject.c_str(), predicate.c_str(), object.c_str());

		while (it->hasNext()) {
			TripleString *triple = it->next();
			//cout << "Result: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
			numResults++;
		}
		delete it;
	} else {
		//get closest IC less than the given version
		std::set<int>::iterator lowerIC = materializedVersions.lower_bound(
				staticVersionQuery);
		if (lowerIC==materializedVersions.end()){ //if all ICs are less than the current version, then go to the last one
			lowerIC--;
		}

		if ((*lowerIC) > staticVersionQuery) { //lower_bound gives where staticVersionQuery is found, or the element after if it is not found
			lowerIC--;
		}
		int closestIC = (*lowerIC);

		//cout << "Not in IC. Closest closestIC is: " << closestIC << endl;
		// get first results from IC
		IteratorTripleString *it = HDTversions[closestIC]->search(
				subject.c_str(), predicate.c_str(), object.c_str());
		while (it->hasNext()) {
			TripleString* triple = it->next();
			//cout << "Result: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
			results.insert(
					triple->getSubject() + " " + triple->getPredicate() + " "
							+ triple->getObject());
		}

		// Now retrieve the Deltas
		for (int i = closestIC + 1; i <= staticVersionQuery; i++) {
			//cout << "search add in i=" << i << endl;
			IteratorTripleString* it_add = HDTversions_add[i]->search(
					subject.c_str(), predicate.c_str(), object.c_str());

			while (it_add->hasNext()) {
				TripleString* triple = it_add->next();
				//cout << "Result: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
				results.insert(
						triple->getSubject() + " " + triple->getPredicate()
								+ " " + triple->getObject());

			}
			//cout << numResults << " results inserted" << endl;
			delete it_add;
			//cout << "search del in i=" << i << endl;

			IteratorTripleString* it_del = HDTversions_del[i]->search(
					subject.c_str(), predicate.c_str(), object.c_str());

			while (it_del->hasNext()) {
				TripleString* tripledel = it_del->next();
				//cout << "Result: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
				results.erase(
						tripledel->getSubject() + " "
								+ tripledel->getPredicate() + " "
								+ tripledel->getObject());

			}
			delete it_del;

			//cout << numResults << " results after deletion" << endl;
		}
		for (std::set<string>::iterator it = results.begin();
				it != results.end(); ++it) {
			//cout << "Final result:" << *it << endl;
			numResults++;
		}
	}

	return numResults;
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
	//	cout << "fileString is: " << fileString << endl;
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
	//	cout << "fileString is: " << fileString << endl;

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
			for (int i = 0; i < numVersions; i++) {
				StopWatch st;
				//cout << endl << endl << "-------- QUERY AT VERSION " << i<< "------------" << endl;
				int numResults = materializeQuery(i, subject, predicate, object);
				double time = st.toMillis();
//				cout << numResults << " results in " << time << " ms" << endl;
				times[i] = times[i] + time;
			}
		}
		num_queries++;
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
