// CSV Merge utility
// Helps merge two CSV files
// By Mike Silverman

#include "stdafx.h"
#include <string>
#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <deque>
#include <algorithm>
#include <map>

#include "..\Common\UtilFuncs.h"
#include "..\Common\CLParams.h"
#include "..\Common\CSVFilter.h"
#include "..\Common\FileOps.h"

static std::deque<std::string *> rowsToProcessQueue;
static std::mutex rowsToProcessMutex;

static std::atomic_bool finishInputs = false;
static std::atomic_bool finishProcThreads = false;

static CLParams globalParams;
static FileOps globalFileOps;

void LoadColumnNames(std::string, std::vector<std::string>&);
int IterateThroughFile(filterParamVectorType&);
long MainInputFileLoop(bool&);
void ProcessRowFunc(filterParamVectorType*);
void ProcessOutputQueueFunc(bool);
void ApplyKeepRemoveCols(std::string*);

// CSVMerge.exe parameters
// -inputf "file name of data to analyze" (Required)
// -inputfsecond "file name of second data to analyze" (Required)
// -outputf "file name of primary output - if filters = true" (Required)
// -outputfother "file name of other output - if filters = false" (optional for when splitting files)
// -processqueuebuffer # of bytes to use for input buffer (default = 1000000000)
// -coltoremove[n] or coltokeep[n] positive or negative list of column names to keep/remove

int main(int argc, char* argv[])
{
	int err = 0;

	inputParamVectorType inputParameters;
	std::string headerRow = "";
	std::vector<std::string> columnInfoFirst;
	std::vector<std::string> columnInfoSecond;
	filterParamVectorType filterInfo;

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
	err = globalFileOps.OpenFiles(inputParameters, globalParams, true);
	if (err != 0) {
		return err;
	}


	// Load column names for filters 
	std::getline(globalFileOps.inFile, headerRow);
	LoadColumnNames(headerRow, columnInfoFirst);
	std::getline(globalFileOps.inFileSecond, headerRow);
	LoadColumnNames(headerRow, columnInfoSecond);

	try {
		globalParams.GiveColNumToNames(columnInfoFirst, true);
		globalParams.GiveColNumToNames(columnInfoSecond, false);
	}
	catch (...) {
		std::cerr << std::endl << "Invalid column name to drop/keep provided." << std::endl;
		err = 10;
	}

	if (err == 0) {
		// Load filters from args
		//err = LoadFilters(filterInfo, inputParameters, columnInfo);
	}

	if (err == 0) {
		// Kick off main loop
		try {
			ApplyKeepRemoveCols(&headerRow);
			globalFileOps.WriteHeaderRow(headerRow);
			//IterateThroughFile(filterInfo);
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
int IterateThroughFile(filterParamVectorType& filterInfo) {
	std::vector<std::thread*> threadPool;
	std::thread* outputNormalThread = nullptr;
	std::thread* outputOtherThread = nullptr;
	unsigned int i = 0;
	unsigned int numThreads = 0;
	bool isOtherOutputThreadNeeded = globalFileOps.outFileOther.is_open();
	unsigned int overheadThreads = (isOtherOutputThreadNeeded ? 3 : 2); // 3 = one input, 2 output, 2 = 1 input and output

																		// setup threads and queues
																		// TODO: Give option for GPU (have to figure out if cost of copying to GPU is worthwhile, maybe only for long running operations such as long functioncalls) 
																		// ANd technically would have to write those functions without STD library.

	numThreads = ((std::thread::hardware_concurrency() - overheadThreads) <= 1 ? 1 : std::thread::hardware_concurrency() - overheadThreads); // minimum 1 thread for work
																																			 //numThreads = 1;
	for (i = 0; i < numThreads; ++i) {
		threadPool.push_back(new std::thread(ProcessRowFunc, &filterInfo));
	}
	outputNormalThread = new std::thread(ProcessOutputQueueFunc, true);
	if (isOtherOutputThreadNeeded) {
		outputOtherThread = new std::thread(ProcessOutputQueueFunc, false);
	}

	// main loop
	long rowsProcessed = MainInputFileLoop(isOtherOutputThreadNeeded);
	finishInputs = true;

	std::cout << "Finished loading " << rowsProcessed << " rows, now finishing processing and writing.                                            \r";

	// clean threads and queues
	for (i = 0; i < numThreads; ++i)
	{
		threadPool[i]->join();
		delete threadPool[i];
	}
	threadPool.clear();

	// Done working now can signal to output threads to stop their work
	finishProcThreads = true;
	outputNormalThread->join();
	if (isOtherOutputThreadNeeded) {
		outputOtherThread->join();
	}

	std::cout << "Finished processing and writing " << rowsProcessed << " rows.                                                   " << std::endl;

	return 0;
}

long MainInputFileLoop(bool& isOtherOutputThreadNeeded) {
	long rowNum = 1l;
	unsigned long long maxRowSize = 0;

	while (!globalFileOps.inFile.eof()) {
		size_t procQueueSize = 0;
		size_t outputNormalQueueSize = 0;
		size_t outputOtherQueueSize = 0;

		// Read data from file
		std::string* rowData = new std::string();  // will get deleted when written to the output file
		std::getline(globalFileOps.inFile, *rowData);
		*rowData = StripQuotesString(*rowData);
		maxRowSize = std::max(maxRowSize, (unsigned long long)rowData->size());

		// check for max buffer size every 5 rows.  Chance to exceed buffer, but limited with such a small # of checks.
		// better performance than checking every cycle

		if (rowNum % 5 == 0) {
			bool atBufferLimit = true;
			do {
				// Get various queue sizes
				rowsToProcessMutex.lock();
				procQueueSize = rowsToProcessQueue.size();
				rowsToProcessMutex.unlock();

				outputNormalQueueSize = globalFileOps.GetQueueSize(true);

				if (isOtherOutputThreadNeeded) {
					outputOtherQueueSize = globalFileOps.GetQueueSize(false);
				}

				//unsigned long long currentQueueSize = ((unsigned long long)maxRowSize * ((unsigned long long)(procQueueSize)) + (unsigned long long)(outputNormalQueueSize) + (unsigned long long)(outputOtherQueueSize));

				if (((unsigned long long)maxRowSize * ((unsigned long long)(procQueueSize)) + (unsigned long long)(outputNormalQueueSize) + (unsigned long long)(outputOtherQueueSize)) > globalParams.processQueueBuffer) {
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
				}
				else {
					atBufferLimit = false;
				}
			} while (atBufferLimit);
		}

		// add to queue for processing
		if ((rowData != nullptr) && (rowData->length() > 0)) {
			rowsToProcessMutex.lock();
			rowsToProcessQueue.push_back(rowData);
			rowsToProcessMutex.unlock();
		}
		// Update user
		if (rowNum % 10000 == 0) {
			std::cout << "Row: " << rowNum << " Proc Queue: " << procQueueSize << " NOutput: " << outputNormalQueueSize << " OOutput: " << outputOtherQueueSize << "              \r";
		}
		++rowNum;
	}
	return rowNum;
}

void ProcessRowFunc(filterParamVectorType* filterInfo) {
	std::string * rowData = nullptr;
	bool keepWorking = true;

	do {
		int keepRow = (int)false;
		bool emptyQueue = true;
		rowsToProcessMutex.lock();
		if (!rowsToProcessQueue.empty()) {
			rowData = rowsToProcessQueue.front();
			rowsToProcessQueue.pop_front();
			rowsToProcessMutex.unlock();
			emptyQueue = false;
		}
		else {
			rowsToProcessMutex.unlock();
		}

		if (!emptyQueue) {
			// Process the row for filtering
			keepRow = (int)true;
			if (filterInfo->size() > 0) {
				//keepRow = ProcessFilterSingleRow(rowData, filterInfo);
				if (keepRow > (int)true) {
					// something went wrong
					delete rowData;
					throw std::runtime_error("Error in ProcessFilterSingleRow");
				}
			}

			if (keepRow == (int)true) {
				// add to normal output queue
				ApplyKeepRemoveCols(rowData);
				globalFileOps.AddDataToOutputQueue(true, rowData);
			}
			else {
				// check if it goes to the "other" file
				if (globalFileOps.outFileOther.is_open()) {
					ApplyKeepRemoveCols(rowData);
					globalFileOps.AddDataToOutputQueue(false, rowData);
				}
				else {
					// not needed, delete
					delete rowData;
					rowData = nullptr;
				}
			}
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


void ProcessOutputQueueFunc(bool isNormalOutput) {

	bool keepWorking = true;

	do {
		std::string* rowData = globalFileOps.GetTopOfQueue(isNormalOutput);

		if (rowData != nullptr) {
			// Process the row
			globalFileOps.WriteOutputRow(isNormalOutput, rowData);
		}
		else {
			if (!finishProcThreads) {
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
			else {
				keepWorking = false;
			}
		}
	} while (keepWorking);
}


void LoadColumnNames(std::string headerRow, std::vector<std::string>& columnInfo) {
	std::string colName = "";
	bool keepAlive = true;

	while (keepAlive) {
		keepAlive = FindAndSplitNextCSVElement(headerRow, colName);
		columnInfo.push_back(StripQuotesString(colName));
	}

}

void ApplyKeepRemoveCols(std::string* rowData) {
	// Check if there's anything to do
	if (globalParams.columnOperations == colNoChange) {
		return;
	}

	std::string newRowData = "";
	size_t foundComma = 0;
	size_t lastFound = 0;
	bool firstOutput = true;
	unsigned int colNumInRow = 0;
	unsigned int nextColToRemoveSpotInList = 0;

	while (nextColToRemoveSpotInList < globalParams.colsToModifyNums.size()) {
		// find next comma
		if (foundComma == 0) {
			foundComma = rowData->find(","); // find first ,
			lastFound = 0;
		}
		else {
			lastFound = foundComma;
			foundComma = rowData->find(",", foundComma + 1); // find the next , from the char after the last found one
		}
		if ((foundComma == std::string::npos) && (nextColToRemoveSpotInList < (globalParams.colsToModifyNums.size() - 1))) {
			// ruh roh! reached end of line somehow before we're ready...
			throw std::runtime_error("Error when stripping commas from row data.");
		}

		if (globalParams.columnOperations == colRemoveAsRemove) {
			if (colNumInRow != globalParams.colsToModifyNums[nextColToRemoveSpotInList]) {
				// add this text back, we're keeping it
				// if they were equal, we'd skip it
				if (foundComma == std::string::npos) {
					newRowData.append(rowData->substr(lastFound));
				}
				else {
					newRowData.append(rowData->substr(lastFound, (foundComma - lastFound)));
				}
			}
			else {
				// found this match
				++nextColToRemoveSpotInList;
			}
		}
		else {
			// it's reversed now for col as keep
			if (colNumInRow == globalParams.colsToModifyNums[nextColToRemoveSpotInList]) {
				// add this text back, we're keeping it
				// if they were not equal, we'd skip it
				if (foundComma == std::string::npos) {
					newRowData.append(rowData->substr(lastFound));
				}
				else {
					newRowData.append(rowData->substr(lastFound, (foundComma - lastFound)));
				}
				++nextColToRemoveSpotInList;
			}
			/*else {
			// basically do nothing if they don't match, we're dropping

			}
			}*/
		}


		++colNumInRow;
	}
	// strip leading and lagging commas
	if (newRowData.substr(0, 1) == ",") {
		newRowData = newRowData.substr(1);
	}
	if (newRowData.substr(newRowData.length() - 1, 1) == ",") {
		newRowData = newRowData.substr(0, newRowData.length() - 1);
	}
	*rowData = newRowData;
}
