//#include "stdafx.h"
#include "FileOps.h"
#include <fstream>
#include <string>
#include "UtilFuncs.h"
#include <iostream>

FileOps::FileOps()
{
}
FileOps::~FileOps()
{
	CloseFiles();
}


int FileOps::OpenFiles(inputParamVectorType& inputParameters, CLParams& params, bool requireSecondInput) {
	// Find Input and output files
	inputFileName = params.FindParamChar("-inputf", inputParameters, 1);
	inputFileNameSecond = params.FindParamChar("-inputfsecond", inputParameters, 1);
	outputFileName = params.FindParamChar("-outputf", inputParameters, 1);
	outputFileNameOther = params.FindParamChar("-outputfother", inputParameters, 1);

	if ((inputFileName == "") || (outputFileName == "")) {
		std::cerr << "Both input and output file names not given." << std::endl;
		return 2;
	}

	if (!OpenSingleFile(inputFileName, inFile)) {
		std::cerr << "Could not open input file." << std::endl;
		return 3;
	}
	if (!OpenSingleFile(outputFileName, outFile)) {
		std::cerr << "Could not open output file." << std::endl;
		return 4;
	}

	if (requireSecondInput && (!OpenSingleFile(inputFileNameSecond, inFileSecond))) {
		// main code will determine, e.g. if merge then needed
		std::cerr << "Could not open second input file." << std::endl;
		return 5;
	}

	OpenSingleFile(outputFileNameOther, outFileOther); // Ok if it doesn't open, not needed perhaps

	return 0;
}

void FileOps::CloseFiles() {
	if (inFile.is_open()) {
		inFile.close();
	}
	if (inFileSecond.is_open()) {
		inFileSecond.close();
	}
	if (outFile.is_open()) {
		outFile.close();
	}
	if (outFileOther.is_open()) {
		outFileOther.close();
	}
}

size_t FileOps::GetQueueSize(bool isNormalOutput)
{
	std::deque<processStruct *>* thisQueue = (isNormalOutput ? &rowsToWriteNormalQueue : &rowsToWriteOtherQueue);
	std::mutex* thisMutex = (isNormalOutput ? &rowsToWriteNormalMutex : &rowsToWriteOtherMutex);

	thisMutex->lock();
	size_t retVal = thisQueue->size();
	thisMutex->unlock();

	return retVal;
}


void FileOps::WriteHeaderRow(std::string& headerRow) {
	processStruct headerProc;
	headerProc.rowData = StripQuotesString(headerRow);

	WriteOutputRow(true, &headerProc, false);
	if (outFileOther.is_open()) {
		WriteOutputRow(false, &headerProc, false);
	}
}

void FileOps::WriteOutputRow(bool isNormalOutput, processStruct* rowStruct, bool deleteRowData) {
	std::ofstream* thisOutfile = (isNormalOutput ? &outFile : &outFileOther);
	std::mutex* thisMutex = (isNormalOutput ? &outputNormalFileWriteMutex : &outputOtherFileWriteMutex);

	thisMutex->lock();
	*thisOutfile << rowStruct->rowData << std::endl;
	thisOutfile->flush();
	thisMutex->unlock();

	if (deleteRowData) {
		delete rowStruct;
	}
}
void FileOps::WriteOutputRow(bool isNormalOutput, std::string* rowData, bool deleteRowData) {
	std::ofstream* thisOutfile = (isNormalOutput ? &outFile : &outFileOther);
	std::mutex* thisMutex = (isNormalOutput ? &outputNormalFileWriteMutex : &outputOtherFileWriteMutex);

	thisMutex->lock();
	*thisOutfile << *rowData << std::endl;
	thisOutfile->flush();
	thisMutex->unlock();

	if (deleteRowData) {
		delete rowData;
	}
}

bool FileOps::OpenSingleFile(std::string& fileName, std::ifstream& inFile) {
	if (fileName.length() > 0) {
		inFile.open(fileName, std::ifstream::in);
	}
	else {
		return false;
	}
	if (inFile.is_open()) {
		return true;
	}
	return false;
}
bool FileOps::OpenSingleFile(std::string& fileName, std::ofstream& outFile) {
	if (fileName.length() > 0) {
		outFile.open(fileName, std::ofstream::out);
	}
	else {
		return false;
	}
	if (outFile.is_open()) {
		return true;
	}
	return false;
}



processStruct* FileOps::GetTopOfQueue(bool isNormalOutput) {
	std::deque<processStruct *>* thisQueue = (isNormalOutput ? &rowsToWriteNormalQueue : &rowsToWriteOtherQueue);
	std::mutex* thisMutex = (isNormalOutput ? &rowsToWriteNormalMutex : &rowsToWriteOtherMutex);

	processStruct* rowData = nullptr;

	thisMutex->lock();
	if (!thisQueue->empty()) {
		rowData = thisQueue->front();
		thisQueue->pop_front();
		thisMutex->unlock();
	}
	else {
		thisMutex->unlock();
	}

	return rowData;
}

void FileOps::AddDataToOutputQueue(bool isNormalOutput, processStruct* procStruct) {
	std::deque<processStruct *>* thisQueue = (isNormalOutput ? &rowsToWriteNormalQueue : &rowsToWriteOtherQueue);
	std::mutex* thisMutex = (isNormalOutput ? &rowsToWriteNormalMutex : &rowsToWriteOtherMutex);

	thisMutex->lock();
	thisQueue->push_back(procStruct);
	thisMutex->unlock();
}

unsigned long long FileOps::GetRowCountFromFile(std::string filename, std::ifstream& inFile, bool skipHeader) {
	unsigned long long rowCount = 0l;
	std::string readRow;

	std::cout << "Retrieving Size of Input File\r";
	do {
		std::getline(inFile, readRow);
		if (readRow.length() > 0) {
			++rowCount;
		}
		if (rowCount % 10000 == 0) {
			std::cout << "Retrieving Size of Input File: " << rowCount << "\r";
		}
	} while ((readRow.length() > 0) && (!inFile.eof()));

	if (skipHeader) {
		--rowCount; // decrement header row
	}
	std::cout << "Finished getting input file size                                  " << std::endl;
	// close and reopen file
	inFile.close();
	inFile.open(filename, std::ios::in);
	return rowCount;
}