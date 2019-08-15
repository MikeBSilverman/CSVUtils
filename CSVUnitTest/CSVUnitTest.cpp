// CSVUnitTest.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

//#include "..\CSVSplit\stdafx.h"
#include "..\Common\CLParams.h"
#include "..\Common\FileOps.h"
#include "..\Common\UtilFuncs.h"
#include <iostream>
#include <atomic>
#include <deque>
#include <vector>
#include <string>
#include <algorithm>
#include <mutex>
#include <map>

static CLParams globalParams;
static FileOps globalFileOps;
static std::atomic_bool finishInputs = false;

static std::deque<processStruct *> rowsToProcessQueue;
static std::mutex rowsToProcessMutex;

long long MainInputFileLoop();
int IterateThroughFile();
void ProcessRowStatsFunc();
void AnalyzeThisRow(std::string&);
void OutputStatistics();

// Constants for program operation
const int queueUpdateSize = 5;
const int outputFrequency = 10000;

// Variables for Statistics Analysis
struct columnStatistics {
	std::map<std::string, long long> uniqueValues;
	bool doesColumnEqualLabel = true;
	std::map<std::string, std::string> mappingThisColToLabel;
};

long labelColNum = -1l;
std::vector<std::string> columnInfo;
std::vector<columnStatistics>* statisticsTable;


// CSVSplit.exe parameters
// -inputf "file name of data to analyze" (Required)
// -outputf "file name of output of statistical analysis" (Required) will be txt output, not CSV
// -labelCol "name of column with the expected output of the model, for comparison" (optional)

int main(int argc, char* argv[])
{
	int err = 0;

	inputParamVectorType inputParameters;
	std::string headerRow = "";


	if (argc < 2) {
		// nothing to run
		std::cerr << "No parameters passed." << std::endl;
		return 1;
	}
	// parse command-line parameters
	globalParams.ParseParameters(argc, argv, inputParameters);
	globalParams.GetOperationalParams(inputParameters);

	if (globalParams.processQueueBuffer == 0) {
		std::cerr << "Error with processQueueBuffer length" << std::endl;
		return 1;
	}

	// open files
	err = globalFileOps.OpenFiles(inputParameters, globalParams);
	if (err != 0) {
		return err;
	}

	// Load column names for filters 
	std::getline(globalFileOps.inFile, headerRow);
	LoadColumnNames(headerRow, columnInfo);

	if (columnInfo.size() == 0) {
		std::cerr << "Error with getting Column Names" << std::endl;
		return 1;
	}

	std::string labelColName;
	globalParams.FindParamString(labelColName, inputParameters, 1); // Ok if blank
	if (labelColName.size() > 0) {
		labelColNum = 0;
		while ((labelColNum < (long)columnInfo.size()) && (columnInfo[labelColNum] != labelColName)) {
			++labelColNum;
		}
		if (labelColNum >= (long)columnInfo.size()) {
			std::cerr << "Error with getting Column Number" << std::endl;
			return 1;
		}
	}

	if (err == 0) {
		// Kick off main loop
		try {
			statisticsTable = new std::vector<columnStatistics>[columnInfo.size()];
			globalFileOps.WriteHeaderRow(headerRow);
			IterateThroughFile();

			OutputStatistics();
			delete[] statisticsTable;
		}
		catch (std::exception& e) {
			std::cerr << std::endl << "Exception encountered.  Terminating before end of input file: " << e.what() << std::endl;
		}
	}

	// close files
	globalFileOps.CloseFiles();

	return 0;
}


// Setup threads for output and processing
// Then loop through the file
int IterateThroughFile() {
	std::vector<std::thread*> threadPool;
	std::thread* outputNormalThread = nullptr;
	std::thread* outputOtherThread = nullptr;
	unsigned int i = 0;
	unsigned int numThreads = 0;
	bool isOtherOutputThreadNeeded = globalFileOps.outFileOther.is_open();
	unsigned int overheadThreads = (isOtherOutputThreadNeeded ? 3 : 2); // 3 = one input, 2 output, 2 = 1 input and output

	// setup threads and queues
	// TODO: Give option for GPU (have to figure out if cost of copying to GPU is worthwhile, maybe only for long running operations such as long functioncalls) 
	// And technically would have to write those functions without STD library.

	numThreads = ((std::thread::hardware_concurrency() - overheadThreads) <= 1 ? 1 : std::thread::hardware_concurrency() - overheadThreads); // minimum 1 thread for work
	//numThreads = 1;
	for (i = 0; i < numThreads; ++i) {
		threadPool.push_back(new std::thread(ProcessRowStatsFunc));
	}
	
	// main loop
	long long rowsProcessed = MainInputFileLoop();

	// signal to worker threads to stop
	finishInputs = true;

	std::cout << "Finished loading " << rowsProcessed << " rows, now finishing processing and writing.                                            \r";

	// clean threads and queues
	for (i = 0; i < numThreads; ++i)
	{
		threadPool[i]->join();
		delete threadPool[i];
	}
	threadPool.clear();

	return 0;
}

long long MainInputFileLoop() {
	long long rowNum = 1l;
	unsigned long long maxRowSize = 0;

	// Iterate through file
	while (!globalFileOps.inFile.eof()) {
		size_t procQueueSize = 0;

		// Read data from file
		processStruct* rowStruct = new processStruct;  // will get deleted when written to the output file
		std::getline(globalFileOps.inFile, rowStruct->rowData);

		rowStruct->rowData = StripQuotesString(rowStruct->rowData);
		maxRowSize = std::max(maxRowSize, (unsigned long long)rowStruct->rowData.size());

		// check for max buffer size every 5 rows.  Chance to exceed buffer, but limited with such a small # of checks.
		// better performance than checking every cycle

		if (rowNum % queueUpdateSize == 0) {
			bool atBufferLimit = true;
			do {
				// Get various queue sizes
				rowsToProcessMutex.lock();
				procQueueSize = rowsToProcessQueue.size();
				rowsToProcessMutex.unlock();

				if (((unsigned long long)maxRowSize * ((unsigned long long)(procQueueSize))) > globalParams.processQueueBuffer) {
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
				}
				else {
					atBufferLimit = false;
				}
			} while (atBufferLimit);
		}

		// add to queue for processing
		if ((rowStruct != nullptr) && (rowStruct->rowData.length() > 0)) {
			// Add to queue
			rowsToProcessMutex.lock();
			rowsToProcessQueue.push_back(rowStruct);
			rowsToProcessMutex.unlock();
		}
		// Update user
		if (rowNum % outputFrequency == 0) {
			std::cout << "Row: " << rowNum << " Waiting to Process Queue: " << procQueueSize << "              \r";
		}
		++rowNum;
	}
	return rowNum;
}

void ProcessRowStatsFunc() {
	processStruct* procStruct = nullptr;
	bool keepWorking = true;

	do {
		bool emptyQueue = true;
		rowsToProcessMutex.lock();
		if (!rowsToProcessQueue.empty()) {
			procStruct = rowsToProcessQueue.front();
			rowsToProcessQueue.pop_front();
			rowsToProcessMutex.unlock();
			emptyQueue = false;
		}
		else {
			rowsToProcessMutex.unlock();
		}

		if (!emptyQueue) {
			_ASSERT(procStruct != nullptr);
			
			// Do analysis
			AnalyzeThisRow(procStruct->rowData);
			
			delete procStruct;
			procStruct = nullptr;
		}
		else {
			if (!finishInputs) {
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
			else {
				keepWorking = false;
			}
		}
	} while (keepWorking);
}

void AnalyzeThisRow(std::string& rowData) {
	
}


void OutputStatistics() {

}