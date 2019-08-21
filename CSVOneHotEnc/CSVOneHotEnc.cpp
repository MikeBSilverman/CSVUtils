// CSV One Hot Encode utility
// Helps take a column in a CSV and expand it in OneHotEncode fashion.  E.g. male/female in one column could introduce bias.  Best to do isMale vs isFemale boolean columns.
// Originally by Mike Silverman, shared under MIT License


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

static std::deque<processStruct *> rowsToProcessQueue;
static std::mutex rowsToProcessMutex;

static std::atomic_bool finishInputs(false);
static std::atomic_bool finishProcThreads(false);


long long MainFileLoop(bool);
int IterateThroughFile(bool);
void ProcessRowEncFunc(bool);
void AnalyzeThisRow(processStruct*);
void ApplyRemoveCol(std::string* );
void GetUpdatedHeader(std::string& headerRow);

void WriteThisRow(processStruct*);
void AddEncodingsToThisRow(std::string*);
void ProcessOutputQueueFunc(bool);

// Constants for program operation
const int queueUpdateSize = 5;
const int outputFrequency = 10000;

// Variables for Statistics Analysis
typedef std::map<std::string, long long> uniqValuesMapType;
struct columnStatistics {
	uniqValuesMapType uniqueValues;
	//bool doesColumnEqualLabel = true;
	//std::map<std::string, std::string> mappingThisColToLabel;
};
long encColNum = -1l;
std::string encColName = "";
std::vector<std::string> columnInfo;
std::vector<columnStatistics> statisticsTable;
std::mutex* statisticsTableMutex;

std::string GetThisValueFromRow(std::string*, size_t&, size_t&, bool);
void GetNextCommasInRow(std::string*, size_t&, size_t&);
std::string GetTheEncValForThisRow(std::string*);
void AddStatsForThisColumn(std::string&);
void AddStatsUniqueVal(columnStatistics*, std::string&);



// CSVUnitTest.exe parameters
// -inputf "file name of data to analyze" (Required)
// -outputf "file name of output of statistical analysis" (Required) will be CSV output
// -colToEnc "name of column to encode" (Required)
// -removeOld remove the original column to encode (optional)

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

	// Load column names 
	std::getline(globalFileOps.inFile, headerRow);
	LoadColumnNames(headerRow, columnInfo);

	if (columnInfo.size() == 0) {
		std::cerr << "Error with getting Column Names" << std::endl;
		return 1;
	}
	
	// Get the column to enumerate

	encColName = globalParams.FindParamChar("-colToEnc", inputParameters, 1); // Not ok if blank
	
	if (encColName.size() == 0) {
		std::cerr << "No encode column specified" << std::endl;
		return 1;
	}

	encColNum = 0;
	while ((encColNum < (long)columnInfo.size()) && (columnInfo[encColNum] != encColName)) {
		++encColNum;
	}
	if (encColNum >= (long)columnInfo.size()) {
		std::cerr << "Error with getting Column Number" << std::endl;
		return 1;
	}

	// check for optional remove encode col
	if (globalParams.FindParamChar("-removeOld", inputParameters, 0) == "-removeOld") {
		globalParams.columnOperations = colRemoveAsRemove;
	}

	if (err == 0) {
		// Kick off main loop
		try {
			// see stats table with blanks
			statisticsTable.resize(1);
			statisticsTableMutex = new std::mutex[1];

			// Loop through the file, collecting stats along the way
			IterateThroughFile(true);
			std::cout << "Found " << statisticsTable[0].uniqueValues.size() << " in column " << encColName << "." << std::endl;
			// Reset input file
			globalFileOps.CloseFiles();
			err = globalFileOps.OpenFiles(inputParameters, globalParams);
			if (err != 0) {
				return err;
			}
			std::getline(globalFileOps.inFile, headerRow);

			// Add new headers
			ApplyRemoveCol(&headerRow);
			GetUpdatedHeader(headerRow);

			// Output New Encodings
			globalFileOps.WriteHeaderRow(headerRow);
			IterateThroughFile(false);

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
int IterateThroughFile(bool initialLoop) {
	std::vector<std::thread*> threadPool;
	std::thread* outputNormalThread = nullptr;

	unsigned int i = 0;
	unsigned int numThreads = 0;
	bool isOtherOutputThreadNeeded = globalFileOps.outFileOther.is_open();
	unsigned int overheadThreads = (isOtherOutputThreadNeeded ? 3 : 2); // 3 = one input, 2 output, 2 = 1 input and output

	// setup threads and queues
	// TODO: Give option for GPU (have to figure out if cost of copying to GPU is worthwhile, maybe only for long running operations such as long functioncalls) 
	// And technically would have to write those functions without STD library.
	finishInputs = false;
	finishProcThreads = false;

	numThreads = ((std::thread::hardware_concurrency() - overheadThreads) <= 1 ? 1 : std::thread::hardware_concurrency() - overheadThreads); // minimum 1 thread for work
	//numThreads = 1;
	for (i = 0; i < numThreads; ++i) {
		threadPool.push_back(new std::thread(ProcessRowEncFunc, initialLoop));
	}
	outputNormalThread = new std::thread(ProcessOutputQueueFunc, true);

	// main loop
	long long rowsProcessed = MainFileLoop(initialLoop);

	// signal to worker threads to stop
	finishInputs = true;

	std::cout << "Finished loading " << rowsProcessed << " rows, now finishing processing.                                      " << std::endl;

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

	return 0;
}

long long MainFileLoop(bool initialLoop) {
	long long rowNum = 1l;
	unsigned long long maxRowSize = 0;

	// Iterate through file
	while (!globalFileOps.inFile.eof()) {
		size_t procQueueSize = 0;
		size_t outputNormalQueueSize = 0;

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

				if (((unsigned long long)maxRowSize * ((unsigned long long)(procQueueSize)) + (unsigned long long)(outputNormalQueueSize)) > globalParams.processQueueBuffer) {
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
			std::cout << (initialLoop ? "Initial" : "Output") << " Loop: Row: " << rowNum << "\tWaiting to Process Queue: " << procQueueSize << "  # Rows queue Normal: " << outputNormalQueueSize << "              \r";
		}
		++rowNum;
	}
	return rowNum;
}

void ProcessRowEncFunc(bool initialLoop) {
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

			if (initialLoop) {
				// Do analysis
				AnalyzeThisRow(procStruct);
				delete procStruct;
				procStruct = nullptr;
			}
			else {
				// Do output
				WriteThisRow(procStruct);
				// delete will happen in the write output
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


void AnalyzeThisRow(processStruct* rowStruct) {

	std::string thisRowEnc;

	// Get the encoding value for this row
	thisRowEnc = GetTheEncValForThisRow(&(rowStruct->rowData));
	
	// Load all 
	AddStatsForThisColumn(thisRowEnc);
		
}

void WriteThisRow(processStruct* rowStruct) {
	AddEncodingsToThisRow(&(rowStruct->rowData));
	ApplyRemoveCol(&(rowStruct->rowData));
	globalFileOps.AddDataToOutputQueue(true, rowStruct);
}


std::string GetTheEncValForThisRow(std::string* rowData) {
	size_t foundComma = 0;
	size_t lastFound = 0;
	long colNumInRow = -1l;

	// Cycle through commas until we find the desired column
	do {
		// find next comma
		GetNextCommasInRow(rowData, foundComma, lastFound);
		if ((foundComma == std::string::npos) && (colNumInRow >= columnInfo.size())) {
			// ruh roh! reached end of line somehow before we're ready...
			throw std::runtime_error("Error when stripping commas from row data.");
		}
		++colNumInRow;
	} while (colNumInRow < encColNum);

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

void AddStatsForThisColumn(std::string& newValue) {

	statisticsTableMutex[0].lock();
	columnStatistics* thisColStats = &(statisticsTable[0]);

	// add if a unique value
	AddStatsUniqueVal(thisColStats, newValue);

	statisticsTableMutex[0].unlock();

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
void AddEncodingsToThisRow(std::string* rowData) {
	std::string thisRowEnc;

	// Get the encoding value for this row
	thisRowEnc = GetTheEncValForThisRow(rowData);

	for (uniqValuesMapType::iterator it = statisticsTable[0].uniqueValues.begin(); it != statisticsTable[0].uniqueValues.end(); ++it) {
		rowData->append(",");
		rowData->append(it->first == thisRowEnc ? "1" : "0");
	}

}

// Based on ApplyKeepRemoveCols in CSVSplit
// This is only a remove, not a keep, so it's simpler
void ApplyRemoveCol(std::string* rowData) {
	// Check if there's anything to do
	if (globalParams.columnOperations == colNoChange) {
		return;
	}

	std::string newRowData = "";
	size_t foundComma = 0;
	size_t lastFound;
	long colNumInRow = 0;

	while (colNumInRow <= encColNum) {
		// find next comma
		if (foundComma == 0) {
			foundComma = rowData->find(","); // find first ,
			lastFound = 0;
		}
		else {
			lastFound = foundComma;
			foundComma = rowData->find(",", foundComma + 1); // find the next , from the char after the last found one
		}
		if ((foundComma == std::string::npos) && (colNumInRow < encColNum)) {
			// ruh roh! reached end of line somehow before we're ready...
			throw std::runtime_error("Error when stripping commas from row data.");
		}

		if (colNumInRow != encColNum) {
			// add this text back, we're keeping it
			// if they were equal, we'd skip it
			if (foundComma == std::string::npos) {
				newRowData.append(rowData->substr(lastFound));
			}
			else {
				newRowData.append(rowData->substr(lastFound, (foundComma - lastFound)));
			}
		}


		++colNumInRow;
	}

	// Add rest of string
	if (foundComma != std::string::npos) {
		newRowData.append(rowData->substr(foundComma));
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

void GetUpdatedHeader(std::string& headerRow) {
	// Add additional columns
	for (uniqValuesMapType::iterator it = statisticsTable[0].uniqueValues.begin(); it != statisticsTable[0].uniqueValues.end(); ++it) {
		headerRow.append(",");
		headerRow.append(encColName);
		headerRow.append(".");
		headerRow.append(it->first);
	}

}