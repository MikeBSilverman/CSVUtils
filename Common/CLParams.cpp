#include "stdafx.h"
#include "CLParams.h"
#include "UtilFuncs.h"
#include <algorithm>

CLParams::CLParams()
{
}

CLParams::~CLParams()
{
}

void CLParams::ParseParameters(int argc, char* argv[], inputParamVectorType& inputParameters) {
	int i = 1;

	while (i < argc) {
		std::string paramToAdd(argv[i]);
		if (paramToAdd[0] == '\"')
		{
			while ((i < (argc - 1)) && (argv[i][::strlen(argv[i]) - 1] != '\"')) {
				++i;
				std::string innerParamToAdd(argv[i]);
				paramToAdd.append(innerParamToAdd);
			}
		}
		else {
			++i;
		}
		inputParameters.push_back(StripQuotesString(paramToAdd));
	}
}


void CLParams::GetOperationalParams(inputParamVectorType& inputParameters) {
	GetParamQueueBuffer(inputParameters);
	GetColsToKeepOrDrop(inputParameters);
}



void CLParams::GetParamQueueBuffer(inputParamVectorType& inputParameters) {
	std::string bufferLength = FindParamChar("-processqueuebuffer", inputParameters, 1);

	if ((bufferLength.length() > 0) && (Is_number(bufferLength))) {
		processQueueBuffer = std::stol(bufferLength);
	}
	else {
		processQueueBuffer = 1000000000l;
	}

	processQueueBuffer -= 20000000l; // overhead
	if (processQueueBuffer < 16000000l) {
		processQueueBuffer = 16000000l; // minimum of 16MB
	}
}

void CLParams::GetColsToKeepOrDrop(inputParamVectorType& inputParameters) {
	std::string filterPrefix = "";

	// Determine what, if any, to do
	if (FindParamChar("-coltoremove1", inputParameters, 0) == "-coltoremove1") {
		columnOperations = colRemoveAsRemove;
		filterPrefix = "-coltoremove";
	}
	else {
		if (FindParamChar("-coltokeep1", inputParameters, 0) == "-coltokeep1") {
			columnOperations = colRemoveAsKeep;
			filterPrefix = "-coltokeep";
		}
		else {
			columnOperations = colNoChange;
			return;
		}
	}

	int i = 1;
	bool keepLoop = true;

	do {
		std::string field = filterPrefix + std::to_string(i);
		std::string nextCol = FindParamString(field, inputParameters, 1);
		if (nextCol.length() == 0) {
			keepLoop = false;
		}
		else {
			colsToModifyNames.push_back(nextCol);
		}
		++i;
	} while (keepLoop);
}

// offset = should it find the param name or another value relative to it?  (e.g. -pname value, 0 = -pname, 1 = value)
std::string CLParams::FindParamString(std::string& param, inputParamVectorType& inputParameters, int offset) {
	return FindParamChar(param.c_str(), inputParameters, offset);
}

std::string CLParams::FindParamChar(const char * param, inputParamVectorType& inputParameters, int offset) {
	unsigned int i = 0;

	while (i < inputParameters.size()) {
		if ((param == inputParameters[i]) && ((i + offset) < inputParameters.size())) {
			if (inputParameters[i + offset].length() > 0) {
				return StripQuotesString(inputParameters[i + offset]);
			}
			else {
				return "";
			}
		}
		else {
			++i;
		}
	}
	return "";
}

// Once we know the actual column names in the input file, we can figure out the column numbers to use
void CLParams::GiveColNumToNames(std::vector<std::string>& columnNames, bool isFirstInput)
{
	inputParamVectorType* namesVec = (isFirstInput ? &colsToModifyNames : &colsToModifyNamesSecond);
	colNumberQueueType* numQueue = (isFirstInput ? &colsToModifyNums : &colsToModifyNumsSecond);
	size_t colToModifyRemaining = namesVec->size();
	std::vector<std::string>::iterator colNameToFindIter = namesVec->begin();

	// loop through the col names in the filter to match against input file names
	while (colToModifyRemaining > 0) {
		bool foundMatch = false;
		unsigned int thisColNumber = 0;

		while ((thisColNumber < columnNames.size()) && !foundMatch) {
			if ((*colNameToFindIter) == columnNames[thisColNumber]) {
				numQueue->push_front(thisColNumber);
				foundMatch = true;
				--colToModifyRemaining;
			}
			else {
				++thisColNumber;
			}
		}
		if (!foundMatch) {
			throw std::runtime_error("Mismatch requested col names to actual col names.");
		}
		++colNameToFindIter;
	}

	// sort col numbers in order
	std::sort(numQueue->begin(), numQueue->end());
}