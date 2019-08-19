// Originally by Mike Silverman, shared under MIT License
#pragma once
#include "CLParams.h"
#include <fstream>
#include <string>
#include <deque>
#include <mutex>

struct processStruct {
	std::string rowData; // used in all cases
	bool writeNormal = true; // used for percentage split (it's decided upfront, not in filter analysis)
};

class FileOps
{
public:
	FileOps();
	~FileOps();

	int OpenFiles(inputParamVectorType&, CLParams&, bool = false);
	void WriteHeaderRow(std::string&);
	void WriteOutputRow(bool, processStruct*, bool = true);
	void WriteOutputRow(bool, std::string*, bool = true);
	void CloseFiles();
	size_t GetQueueSize(bool);
	processStruct* GetTopOfQueue(bool);
	void AddDataToOutputQueue(bool, processStruct*);
	unsigned long long GetRowCountFromFile(std::string, std::ifstream&, bool = true);

	std::ifstream inFile;
	std::string inputFileName;
	unsigned long long inputFileRows = 0l;
	std::ifstream inFileSecond;
	std::string inputFileNameSecond;
	std::ofstream outFile;
	std::string outputFileName;
	std::ofstream outFileOther;
	std::string outputFileNameOther;

	std::deque<processStruct *> rowsToWriteNormalQueue;
	std::deque<processStruct *> rowsToWriteOtherQueue;
	std::mutex rowsToWriteNormalMutex;
	std::mutex rowsToWriteOtherMutex;
	std::mutex outputNormalFileWriteMutex;
	std::mutex outputOtherFileWriteMutex;

private:
	bool OpenSingleFile(std::string&, std::ifstream&);
	bool OpenSingleFile(std::string&, std::ofstream&);
};