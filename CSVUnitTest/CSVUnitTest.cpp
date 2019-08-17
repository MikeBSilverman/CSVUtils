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
#include <set>

static CLParams globalParams;
static FileOps globalFileOps;
static std::atomic_bool finishInputs = false;

static std::deque<processStruct *> rowsToProcessQueue;
static std::mutex rowsToProcessMutex;

long long MainInputFileLoop();
int IterateThroughFile();
void ProcessRowStatsFunc();
void AnalyzeThisRow(std::string*);


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
std::vector<columnStatistics> statisticsTable;
std::mutex* statisticsTableMutex;
const float thresholdForIssueWithUniqueValCount = .5f; // .5 = 50% increase over one another

std::string GetThisValueFromRow(std::string*, size_t&, size_t&, bool);
void GetNextCommasInRow(std::string*, size_t&, size_t&);
std::string GetTheLabelForThisRow(std::string*);
void AddStatsForThisColumn(long&, std::string&, std::string&);
void AddStatsUniqueVal(columnStatistics*, std::string&);
void AddStatsColToLabel(columnStatistics*, std::string&, std::string&);

void OutputStatistics();
long long OutputStatsColMatchLabel();
long long OutputStatsColWithOnlyOneValue();
long long OutputStatsCheckSplitForUniqueValues(size_t);
void OutputUniqueStats();
void OutputStatsWriteSingleLine(size_t, std::string);

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
	labelColName = globalParams.FindParamChar("-labelCol", inputParameters, 1); // Ok if blank
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
			// see stats table with blanks
			statisticsTable.resize(columnInfo.size());
			if (labelColNum >= 0) {
				statisticsTable[labelColNum].doesColumnEqualLabel = false; // don't do analysis on the label column!
			}
			statisticsTableMutex = new std::mutex[columnInfo.size()];

			// Loop through the file, collecting stats along the way
			IterateThroughFile();

			// Output the analysis
			OutputStatistics();

			// Cleanup
			delete[] statisticsTableMutex;
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
		if (rowStruct->rowData.length() > 0) {
			// Add to queue
			rowsToProcessMutex.lock();
			rowsToProcessQueue.push_back(rowStruct);
			rowsToProcessMutex.unlock();
		}
		// Update user
		if (rowNum % outputFrequency == 0) {
			std::cout << "Row: " << rowNum << "\tWaiting to Process Queue: " << procQueueSize << "              \r";
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
			AnalyzeThisRow(&(procStruct->rowData));
			
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

void AnalyzeThisRow(std::string* rowData) {

	std::string newValue;
	std::string thisRowLabel;

	size_t foundComma = 0;
	size_t lastFound = 0;
	long colNumInRow = 0;

	// Get the label for this row
	thisRowLabel = GetTheLabelForThisRow(rowData);
	// Load all 

	while (colNumInRow < columnInfo.size()) {
		// find next comma
		GetNextCommasInRow(rowData, foundComma, lastFound);
		if ((foundComma == std::string::npos) && (colNumInRow < (columnInfo.size() - 1))) {
			// ruh roh! reached end of line somehow before we're ready...
			throw std::runtime_error("Error when stripping commas from row data.");
		}

		// Get the value
		newValue = GetThisValueFromRow(rowData, foundComma, lastFound, colNumInRow == 0);
		
		AddStatsForThisColumn(colNumInRow, thisRowLabel, newValue);

		++colNumInRow;
	}
}


void OutputStatistics() {

	long long results;
	std::cout << std::endl;

	// Check for col matching label 1:1
	if (labelColNum >= 0) {
		results = OutputStatsColMatchLabel();
		std::cout << "Checking for columns where it matched 1:1 with the label column found " << results << " issues." << std::endl;
	}

	// Check if any columns have only one value (likely an error, or just not needed)
	results = OutputStatsColWithOnlyOneValue();
	std::cout << "Checking for columns with only one value found " << results << " issues." << std::endl;

	// May or may not be an issue, but good to check (male vs. female might be a problem, while a one-hot encode may not be an issue)
	results = OutputStatsCheckSplitForUniqueValues(2);
	std::cout << "Checking for columns with two unique values found " << results << " times where the ratio is quite poor." << std::endl;

	// May or may not be an issue, but good to check (male vs. female might be a problem, while a one-hot encode may not be an issue)
	results = OutputStatsCheckSplitForUniqueValues(3);
	std::cout << "Checking for columns with three unique values found " << results << " times where the ratio is quite poor." << std::endl;

	OutputUniqueStats();
}

std::string GetTheLabelForThisRow(std::string* rowData) {
	size_t foundComma = 0;
	size_t lastFound = 0;
	long colNumInRow = -1l;

	// Cycle through commas until we find the desired column
	do {
		// find next comma
		GetNextCommasInRow(rowData, foundComma, lastFound);
		if ((foundComma == std::string::npos) && (colNumInRow < (columnInfo.size() - 1))) {
			// ruh roh! reached end of line somehow before we're ready...
			throw std::runtime_error("Error when stripping commas from row data.");
		}
		++colNumInRow;
	} while (colNumInRow < labelColNum);

	// Get the value
	return GetThisValueFromRow(rowData, foundComma, lastFound, colNumInRow == 0);

}

void GetNextCommasInRow(std::string* rowData, size_t& foundComma, size_t& lastFound) {
	if (foundComma == 0) {
		foundComma = rowData->find(","); // find first ,
		lastFound = 0;
	}
	else {
		lastFound = foundComma;
		foundComma = rowData->find(",", foundComma + 1); // find the next , from the char after the last found one
	}
}
// Get the value from this part of the row
std::string GetThisValueFromRow(std::string* rowData, size_t& foundComma, size_t& lastFound, bool firstString) {
	if (foundComma == std::string::npos) {
		return rowData->substr(lastFound + 1);
	}
	else {
		if (firstString) {
			return rowData->substr(lastFound, (foundComma - lastFound));
		}
		else {
			return rowData->substr(lastFound + 1, (foundComma - lastFound) - 1);
		}
	}
}

void AddStatsForThisColumn(long& colNumInRow, std::string& thisRowLabel, std::string& newValue) {

	statisticsTableMutex[colNumInRow].lock();
	columnStatistics* thisColStats = &(statisticsTable[colNumInRow]);

	// add if a unique value
	AddStatsUniqueVal(thisColStats, newValue);

	// check if the label column and this column are in lockstep
	if ((labelColNum >= 0) && (thisColStats->doesColumnEqualLabel)) {
		AddStatsColToLabel(thisColStats, thisRowLabel, newValue);
	}
	
		statisticsTableMutex[colNumInRow].unlock();
	
}

void AddStatsUniqueVal(columnStatistics* thisColStats, std::string& newValue) {
	std::map<std::string, long long>::iterator itUV = thisColStats->uniqueValues.find(newValue);

	// is the value in the table already?
	if (itUV == thisColStats->uniqueValues.end()) {
		// no, add this value
		thisColStats->uniqueValues[newValue] = 1l;
	}
	else {
		// increment # times we've seen this value
		++itUV->second;
	}
}

void AddStatsColToLabel(columnStatistics* thisColStats, std::string& thisRowLabel, std::string& newValue) {
	// have we already come across this label before
	std::map<std::string, std::string>::iterator itCL = std::find_if(thisColStats->mappingThisColToLabel.begin(), thisColStats->mappingThisColToLabel.end(),
		map_second_value_equals<std::string, std::string>(thisRowLabel));
	if (itCL != thisColStats->mappingThisColToLabel.end()) {
		// we've seen this label before, check if the newValue is consistent
		if (itCL->first != newValue) {
			// no, it's the same value/label paring, and call off the search
			thisColStats->doesColumnEqualLabel = false;
		}
	}
	else {
		// ok, this label hasn't been found yet.  Have we seen this newVal though?
		itCL = thisColStats->mappingThisColToLabel.find(newValue);
		
		if (itCL != thisColStats->mappingThisColToLabel.end()) {
			// yes, we've seen this value, and it doesn't map to the previous label we found
			// call off the search
			thisColStats->doesColumnEqualLabel = false;
		}
		else {
			// we've never seen this value/label pair, and add
			thisColStats->mappingThisColToLabel[newValue] = thisRowLabel;
		}
	}
}

long long OutputStatsColMatchLabel() {
	long long instances = 0; 
	for (size_t col = 0; col < statisticsTable.size(); ++col) {
		if (statisticsTable[col].doesColumnEqualLabel) {
			OutputStatsWriteSingleLine(col, " is in lock step with the label column!");
			++instances;
		}
	}
	return instances;
}

long long OutputStatsColWithOnlyOneValue() {
	long long instances = 0;
	for (size_t col = 0; col < statisticsTable.size(); ++col) {
		if (statisticsTable[col].uniqueValues.size() == 1) {
			OutputStatsWriteSingleLine(col, " has only one value!");
			++instances;
		}
	}
	return instances;
}

long long OutputStatsCheckSplitForUniqueValues(size_t numUniqueChk) {
	long long instances = 0;
	for (size_t col = 0; col < statisticsTable.size(); ++col) {
		if (statisticsTable[col].uniqueValues.size() == numUniqueChk) {
			// step through the list of unique vals and compare
			long long maxWatermark = 0l;
			std::string maxWatermarkVal;
			std::map<std::string, long long>::iterator* iters = new std::map<std::string, long long>::iterator[numUniqueChk];

			for (size_t iterCount = 0; iterCount < numUniqueChk; ++iterCount) {
				iters[iterCount] = statisticsTable[col].uniqueValues.begin();
				size_t i = 0;
				while (i < iterCount) {
					++iters[iterCount];
					++i;
				}

				// see if a new high value
				maxWatermark = std::max(maxWatermark, iters[iterCount]->second);
				if (maxWatermark == iters[iterCount]->second) {
					maxWatermarkVal = iters[iterCount]->first;
				}
			}
			
			for (size_t iterCount = 0; iterCount < numUniqueChk; ++iterCount) {
				if ((iters[iterCount]->second * (thresholdForIssueWithUniqueValCount + 1.0f)) < maxWatermark) {
					std::string outputComplex = " unique value '";
					outputComplex.append(iters[iterCount]->first);
					outputComplex.append("' is below threshold: ");
					outputComplex.append(std::to_string(iters[iterCount]->second));
					outputComplex.append(" vs. ");
					outputComplex.append(std::to_string(maxWatermark));
					outputComplex.append(" times seeing '");
					outputComplex.append(maxWatermarkVal);
					outputComplex.append("'");
					OutputStatsWriteSingleLine(col, outputComplex);
					++instances;
				}
			}
			delete[] iters;
		}
	}
	return instances;
}

void OutputUniqueStats() {
	std::string outputComplex;
	for (size_t col = 0; col < statisticsTable.size(); ++col) {
		outputComplex = "";

		// create a new set for sorting the unique values in descending order
		std::set<std::pair<std::string, long long>, std::function<bool(std::pair<std::string, long long>, std::pair<std::string, long long>)>> sortedUniques( 
			statisticsTable[col].uniqueValues.begin(), statisticsTable[col].uniqueValues.end(),
			[](std::pair<std::string, long long> elem1, std::pair<std::string, long long> elem2) { return elem1.second > elem2.second; });

		// iterate through set and append to output string
		for (std::pair<std::string, long long> uniqueValue : sortedUniques) {
			outputComplex.append(",");
			outputComplex.append(uniqueValue.first);
			outputComplex.append(",");
			outputComplex.append(std::to_string(uniqueValue.second));
		}

		// Write this column's stats
		OutputStatsWriteSingleLine(col, outputComplex);
	}
}

void OutputStatsWriteSingleLine(size_t colNum, std::string msgToOutput) {
	std::string outputData = columnInfo[colNum];
	outputData.append(msgToOutput);
	globalFileOps.WriteOutputRow(true, &outputData, false);
}
