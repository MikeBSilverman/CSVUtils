#pragma once
//#include "stdafx.h"
#include "CLParams.h"
#include <fstream>
#include <string>
#include <deque>
#include <mutex>
class FileOps
{
public:
	FileOps();
	~FileOps();

	int OpenFiles(inputParamVectorType&, CLParams&, bool = false);
	void WriteHeaderRow(std::string&);
	void WriteOutputRow(bool, std::string*, bool = true);
	void CloseFiles();
	size_t GetQueueSize(bool);
	std::string* GetTopOfQueue(bool);
	void AddDataToOutputQueue(bool, std::string*);

	std::ifstream inFile;
	std::ifstream inFileSecond;
	std::ofstream outFile;
	std::ofstream outFileOther;

	std::deque<std::string *> rowsToWriteNormalQueue;
	std::deque<std::string *> rowsToWriteOtherQueue;
	std::mutex rowsToWriteNormalMutex;
	std::mutex rowsToWriteOtherMutex;
	std::mutex outputNormalFileWriteMutex;
	std::mutex outputOtherFileWriteMutex;

private:
	bool OpenSingleFile(std::string&, std::ifstream&);
	bool OpenSingleFile(std::string&, std::ofstream&);
};