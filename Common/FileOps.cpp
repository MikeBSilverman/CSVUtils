#include "stdafx.h"
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
	std::string inputFileName = params.FindParamChar("-inputf", inputParameters, 1);
	std::string inputFileNameSecond = params.FindParamChar("-inputfsecond", inputParameters, 1);
	std::string outputFileName = params.FindParamChar("-outputf", inputParameters, 1);
	std::string outputFileNameOther = params.FindParamChar("-outputfother", inputParameters, 1);

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
	std::deque<std::string *>* thisQueue = (isNormalOutput ? &rowsToWriteNormalQueue : &rowsToWriteOtherQueue);
	std::mutex* thisMutex = (isNormalOutput ? &rowsToWriteNormalMutex : &rowsToWriteOtherMutex);

	thisMutex->lock();
	size_t retVal = thisQueue->size();
	thisMutex->unlock();

	return retVal;
}


void FileOps::WriteHeaderRow(std::string& headerRow) {
	std::string workingHeaderRow = headerRow;
	workingHeaderRow = StripQuotesString(workingHeaderRow);

	WriteOutputRow(true, &workingHeaderRow, false);
	if (outFileOther.is_open()) {
		WriteOutputRow(false, &workingHeaderRow, false);
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



std::string* FileOps::GetTopOfQueue(bool isNormalOutput) {
	std::deque<std::string *>* thisQueue = (isNormalOutput ? &rowsToWriteNormalQueue : &rowsToWriteOtherQueue);
	std::mutex* thisMutex = (isNormalOutput ? &rowsToWriteNormalMutex : &rowsToWriteOtherMutex);

	std::string* rowData = nullptr;

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

void FileOps::AddDataToOutputQueue(bool isNormalOutput, std::string* rowData) {
	std::deque<std::string *>* thisQueue = (isNormalOutput ? &rowsToWriteNormalQueue : &rowsToWriteOtherQueue);
	std::mutex* thisMutex = (isNormalOutput ? &rowsToWriteNormalMutex : &rowsToWriteOtherMutex);

	thisMutex->lock();
	thisQueue->push_back(rowData);
	thisMutex->unlock();
}