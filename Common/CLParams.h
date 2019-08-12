#pragma once
//#include "..\stdafx.h"
#include <vector>
#include <string>
#include <deque>

typedef std::vector<std::string> inputParamVectorType;
typedef std::deque<unsigned int> colNumberQueueType;

enum colOperations {
	colNotDefined,
	colNoChange,
	colRemoveAsKeep,
	colRemoveAsRemove
};

class CLParams
{
public:
	CLParams();
	~CLParams();

	void ParseParameters(int, char*[], inputParamVectorType&);
	std::string FindParamString(std::string&, inputParamVectorType&, int);
	std::string FindParamChar(const char *, inputParamVectorType&, int);
	void GetOperationalParams(inputParamVectorType&);
	void GiveColNumToNames(std::vector<std::string>&, bool = true);

	bool cleanExtraQuotesParam = false;
	unsigned long long processQueueBuffer = 0l;
	inputParamVectorType colsToModifyNames;
	inputParamVectorType colsToModifyNamesSecond;
	colNumberQueueType colsToModifyNums;
	colNumberQueueType colsToModifyNumsSecond;
	colOperations columnOperations = colNotDefined;

private:
	void GetParamQueueBuffer(inputParamVectorType&);
	void GetColsToKeepOrDrop(inputParamVectorType&);

};

