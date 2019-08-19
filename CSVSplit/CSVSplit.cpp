// CSV Split utility
// Helps filter rows of CSV files, split into filter/non-filtered, select certain columns, and more
// Originally by Mike Silverman, shared under MIT License

#include <string>
#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <deque>
#include <algorithm>
#include <map>
#include <random>
#include "..\Common\UtilFuncs.h"
#include "..\Common\CLParams.h"
#include "..\Common\CSVFilter.h"
#include "..\Common\FileOps.h"

enum jobType {
	jobUseFilters,
	jobUsePercentage,
	jobUseUnknown
};

static std::deque<processStruct *> rowsToProcessQueue;
static std::mutex rowsToProcessMutex;

static std::atomic_bool finishInputs = false;
static std::atomic_bool finishProcThreads = false;

static filterOpMap mapFilterOpValues;
static filterJoinOpMap mapFilterJoinOpValues;
static CLParams globalParams;
static FileOps globalFileOps;
static std::mt19937 randGenerator(std::random_device{}());


int IterateThroughFile(jobType, filterParamVectorType&);
long long MainInputFileLoop(bool&, jobType);
int ProcessFilterSingleRow(std::string*, filterParamVectorType*);
void ProcessRowFilterFunc(filterParamVectorType*);
void ProcessRowPercentageFunc();
void ProcessOutputQueueFunc(bool);
void ApplyKeepRemoveCols(std::string*);
void GenerateListOfRowsToSplit(std::deque<long long>&);

// Constants for program operation
const int queueUpdateSize = 5;
const int outputFrequency = 10000;


 // CSVSplit.exe parameters
// -inputf "file name of data to analyze" (Required)
// -outputf "file name of primary output - if filters = true" (Required)
// -outputfother "file name of other output - if filters = false" (optional for when splitting files)
// -filter[n] 
//		"Variable to filter on" (Required) 
//		operand (eq, ne, lt, le, gt, ge) (Required)
//		value to search on (Required)
//		join operand (AND, OR) (Required for 1 to n-1 filters)
//		e.g. -filter1 Year ge 2009 AND -filter2 Year le 2014
// -processqueuebuffer # of bytes to use for input buffer (default = 1000000000)
// -coltoremove[n] or coltokeep[n] positive or negative list of column names to keep/remove
// -percentagesplit .xx  - e.g. if .80 then 80% goes into normal file, remainder 20% will go into other file

int main(int argc, char* argv[])
{
	int err = 0;

	inputParamVectorType inputParameters;
	std::string headerRow = "";
	std::vector<std::string> columnInfo;
	filterParamVectorType filterInfo;
	jobType jobToUse = jobUseUnknown;

	if (argc < 2) {
		// nothing to run
		std::cerr << "No parameters passed." << std::endl;
		return 1;
	}
	// parse command-line parameters
	globalParams.ParseParameters(argc, argv, inputParameters);
	InitializeFilterOperandsEnum(mapFilterOpValues);
	InitializeFilterJoinOperandsEnum(mapFilterJoinOpValues);
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

	try {
		globalParams.GiveColNumToNames(columnInfo);
	}
	catch (...) {
		std::cerr << std::endl << "Invalid column name to drop/keep provided." << std::endl;
		err = 10;
	}

	if (err == 0) {
		// see if a percentage split vs. a filter (can be only one)
		globalParams.GetPercentageSplit(inputParameters);
		if (globalParams.percentageSplit > 0.0f) {
			// Get file length
			globalFileOps.inputFileRows = globalFileOps.GetRowCountFromFile(globalFileOps.inputFileName, globalFileOps.inFile, false);
			std::getline(globalFileOps.inFile, headerRow); // reopened the file, so skip ahead
			jobToUse = jobUsePercentage;
		}
		else {
			// not a split
			// Load filters from args
			err = LoadFilters(filterInfo, inputParameters, columnInfo);
			jobToUse = jobUseFilters;
		}
	}

	if (err == 0) {
		// Kick off main loop
		try {
			ApplyKeepRemoveCols(&headerRow);
			globalFileOps.WriteHeaderRow(headerRow);
			IterateThroughFile(jobToUse, filterInfo);
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
int IterateThroughFile(jobType jobTypeToProc, filterParamVectorType& filterInfo) {
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
		switch (jobTypeToProc) {
		case jobUseFilters:
			threadPool.push_back(new std::thread(ProcessRowFilterFunc, &filterInfo));
			break;
		case jobUsePercentage:
			threadPool.push_back(new std::thread(ProcessRowPercentageFunc));
			break;
		case jobUseUnknown:
		default:
			throw std::runtime_error("Unknown Job Type");
			break;
		}
	}
	outputNormalThread = new std::thread(ProcessOutputQueueFunc, true);
	if (isOtherOutputThreadNeeded) {
		outputOtherThread = new std::thread(ProcessOutputQueueFunc, false);
	}

	// main loop
	long long rowsProcessed = MainInputFileLoop(isOtherOutputThreadNeeded, jobTypeToProc);

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

	// Done working now can signal to output threads to stop their work
	finishProcThreads = true;
	outputNormalThread->join();
	if (isOtherOutputThreadNeeded) {
		outputOtherThread->join();
	}

	std::cout << "Finished processing and writing " << rowsProcessed << " rows.                                                   " << std::endl;
	
	return 0;
}

long long MainInputFileLoop(bool& isOtherOutputThreadNeeded, jobType jobTypeToProc) {
	long long rowNum = 1l;
	unsigned long long maxRowSize = 0;
	std::deque<long long> listOfRowsToSplitToOtherFile;
	
	// Determine which rows (randomly) get split off
	GenerateListOfRowsToSplit(listOfRowsToSplitToOtherFile);

	// Iterate through file
	while (!globalFileOps.inFile.eof()) {
		size_t procQueueSize = 0;
		size_t outputNormalQueueSize = 0;
		size_t outputOtherQueueSize = 0;
		
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

				outputNormalQueueSize = globalFileOps.GetQueueSize(true);

				if (isOtherOutputThreadNeeded) {
					outputOtherQueueSize = globalFileOps.GetQueueSize(false);
				}

				if (((unsigned long long)maxRowSize * ((unsigned long long)(procQueueSize)) + (unsigned long long)(outputNormalQueueSize) + (unsigned long long)(outputOtherQueueSize)) > globalParams.processQueueBuffer) {
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
				}
				else {
					atBufferLimit = false;
				}
			} while (atBufferLimit);
		}

		// add to queue for processing
		if (rowStruct->rowData.length() > 0) {
			// check if a percentage split job
			//rowStruct->writeNormal = true;  // set to true by default

			if (jobTypeToProc == jobUsePercentage) {
				// split this row off?
				if ((listOfRowsToSplitToOtherFile.size() > 0) && (listOfRowsToSplitToOtherFile[0] == rowNum)) {
					rowStruct->writeNormal = false;
					listOfRowsToSplitToOtherFile.pop_front();
				}
			}

			// Add to queue
			rowsToProcessMutex.lock();
			rowsToProcessQueue.push_back(rowStruct);
			rowsToProcessMutex.unlock();
		}
		// Update user
		if (rowNum % outputFrequency == 0) {
			std::cout << "Row: " << rowNum << " Waiting to Process Queue: " << procQueueSize << "  # Rows queue Normal: " << outputNormalQueueSize << "  # Rows queue Other: " << outputOtherQueueSize << "              \r";
		}
		++rowNum;
	}
	return rowNum;
}

void ProcessRowFilterFunc(filterParamVectorType* filterInfo) {
	
	processStruct* procStruct;
	bool keepWorking = true;
	
	do {
		int keepRow = (int)false;
		bool emptyQueue = true;
		rowsToProcessMutex.lock();
		procStruct = nullptr;
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
			// Process the row for filtering
			keepRow = (int)true;
			if (filterInfo->size() > 0) {
				_ASSERT(procStruct != nullptr);
				keepRow = ProcessFilterSingleRow(&procStruct->rowData, filterInfo);
				if (keepRow > (int)true) {
					// something went wrong
					delete procStruct;
					throw std::runtime_error("Error in ProcessFilterSingleRow");
				}
			} 

			if (keepRow == (int)true) {
				// add to normal output queue
				ApplyKeepRemoveCols(&procStruct->rowData);
				globalFileOps.AddDataToOutputQueue(true, procStruct);
			}
			else {
				// check if it goes to the "other" file
				if (globalFileOps.outFileOther.is_open()) {
					ApplyKeepRemoveCols(&procStruct->rowData);
					globalFileOps.AddDataToOutputQueue(false, procStruct);
				}
				else {
					// not needed, delete
					delete procStruct;
					procStruct = nullptr;
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


void ProcessRowPercentageFunc() {

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
			if ((procStruct != nullptr) && (procStruct->writeNormal == true)) {
				// add to normal output queue
				ApplyKeepRemoveCols(&procStruct->rowData);
				globalFileOps.AddDataToOutputQueue(true, procStruct);
			}
			else {
				// check if it goes to the "other" file
				if (globalFileOps.outFileOther.is_open()) {
					ApplyKeepRemoveCols(&procStruct->rowData);
					globalFileOps.AddDataToOutputQueue(false, procStruct);
				}
				else {
					// not needed, delete
					delete procStruct;
					procStruct = nullptr;
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
		processStruct* procStruct = globalFileOps.GetTopOfQueue(isNormalOutput);

		if (procStruct != nullptr) {
			// Process the row
			globalFileOps.WriteOutputRow(isNormalOutput, procStruct);
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

// output: true = put in "normal" output, false = put in "other" output
int ProcessFilterSingleRow(std::string* rowData, filterParamVectorType* ptrFilterInfo) {
	std::deque<bool> interimResults;

	for (size_t i = 0; i < ptrFilterInfo->size(); ++i) {
		// get value for this column in the string
		std::string stringToCheck;
		if (!FindASpecificCSVElement(*rowData, (*ptrFilterInfo)[i].colNum, stringToCheck)) {
			std::cerr << "Could not find colNum " << (*ptrFilterInfo)[i].colNum << std::endl;
			return 7;
		}
		
		// check the op for the must be numeric opers
		std::string operand = (*ptrFilterInfo)[i].op;
		if ((operand == "le") || (operand == "lt") || (operand == "ge") || (operand == "gt")) {
			if (!Is_number(stringToCheck)) {
				std::cerr << stringToCheck << " is not a number, cannot perform " << operand << std::endl;
				return 7;
			}
			switch (mapFilterOpValues[operand]) {
				case filterOpLE:
					interimResults.push_back(std::stod(stringToCheck) <= std::stod((*ptrFilterInfo)[i].value));
					break;
				case filterOpLT:
					interimResults.push_back(std::stod(stringToCheck) < std::stod((*ptrFilterInfo)[i].value));
					break;
				case filterOpGE:
					interimResults.push_back(std::stod(stringToCheck) >= std::stod((*ptrFilterInfo)[i].value));
					break;
				case filterOpGT:
					interimResults.push_back(std::stod(stringToCheck) > std::stod((*ptrFilterInfo)[i].value));
					break;
			}
		}
		else {
			// Down to = or !=
			// check if working with numbers or not
			if (!Is_number(stringToCheck)) {
				// Not Numbers
				switch (GetFilterOperand(operand, mapFilterOpValues)) {
				case filterOpEQ:
					interimResults.push_back(stringToCheck == (*ptrFilterInfo)[i].value);
					break;
				case filterOpNE:
					interimResults.push_back(!(stringToCheck == (*ptrFilterInfo)[i].value));
					break;
				default:
					std::cerr << "Unknown operand: " << operand << std::endl;
					return 7;
					break;
				}
			}
			else {
				// numbers!
				switch (GetFilterOperand(operand, mapFilterOpValues)) {
				case filterOpEQ:
					interimResults.push_back(std::stod(stringToCheck) == std::stod((*ptrFilterInfo)[i].value));
					break;
				case filterOpNE:
					interimResults.push_back(!(std::stod(stringToCheck) == std::stod((*ptrFilterInfo)[i].value)));
					break;
				default:
					std::cerr << "Unknown operand: " << operand << std::endl;
					return 7;
					break;
				}
			}
		}
	}

	// Process results
	size_t j = 0;
	bool retVal = false;

	do {
		if (j == 0) {
			retVal = interimResults[0];
		}
		//if (interimResults[0]) {
		//	retVal = true;
		//}
		if ((j + 1) < interimResults.size()) {
			switch (GetFilterJoinOp((*ptrFilterInfo)[j].joinToNextFilter, mapFilterJoinOpValues)) {
			case filterJoinOpAND:
				retVal = retVal && interimResults[j + 1];
				break;
			case filterJoinOpOR:
				retVal = retVal || interimResults[j + 1];
				break;
			default:
				std::cerr << "Unknown join operand: " << (*ptrFilterInfo)[j - 1].joinToNextFilter << std::endl;
				return 7;
			}
		}
		++j;
	} while (j < (interimResults.size() - 1));
	
	return (int)retVal;
}


void ApplyKeepRemoveCols(std::string* rowData) {
	// Check if there's anything to do
	if (globalParams.columnOperations == colNoChange) {
		return;
	}

	std::string newRowData = "";
	size_t foundComma = 0;
	size_t lastFound;
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

		// Add rest of string if remove inclusive
		if ((nextColToRemoveSpotInList >= globalParams.colsToModifyNums.size()) && (globalParams.columnOperations == colRemoveAsRemove) && (foundComma != std::string::npos)) {
			newRowData.append(rowData->substr(foundComma));
		}
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

void GenerateListOfRowsToSplit(std::deque<long long>& listOfSplits) {
	long long numRowsToSplit = (long long)(round(globalFileOps.inputFileRows * (1.0f - globalParams.percentageSplit)));
	std::uniform_real_distribution<double> randomRow(1.0, (double)globalFileOps.inputFileRows - 1.0);

	std::cout << "Generating List of Rows to Split\r";

	while ((long long)(listOfSplits.size()) <= numRowsToSplit) {
		
		// add numbers to the desired size, even with possible duplicates
		std::cout << "Adding numbers to List of Rows to Split                                                      \r";
		while ((long long)(listOfSplits.size()) <= numRowsToSplit) {
			long long newRowNum = (long long)(round(randomRow(randGenerator)));
			listOfSplits.push_back(newRowNum);
		}

		// sort the random numbers
		std::cout << "Sorting List of Rows to Split                                                      \r";
		std::sort(listOfSplits.begin(), listOfSplits.end());
		std::cout << "Removing duplicates in the list                                                      \r";
		std::deque<long long>::iterator it = std::unique(listOfSplits.begin(), listOfSplits.end());
		
		// resize if needed
		listOfSplits.resize(std::distance(listOfSplits.begin(), it));
	}
	
	std::cout << "Finalized List of Rows to Split: " << listOfSplits.size() << std::endl;
}