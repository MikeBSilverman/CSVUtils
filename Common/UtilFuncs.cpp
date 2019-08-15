//#include "stdafx.h"
#include <string>
#include <algorithm>
#include <cctype>
#include <vector>

// return true = more to do, false = done
bool FindAndSplitNextCSVElement(std::string& csvRow, std::string& element)
{
	std::size_t found = 0;
	element = "";

	if (csvRow.length() == 0) {
		return false;
	}

	found = csvRow.find(",");
	if (found != std::string::npos) {
		// a blank element
		if (found == 0) {
			element = "";
			return true;
		}
		else {
			element = csvRow.substr(0, found);
			csvRow = csvRow.substr(found + 1);
			return true;
		}
	}

	// last bit of CSV string
	element = csvRow.substr(0, found);
	csvRow = "";
	return false;
}
bool FindASpecificCSVElement(std::string& csvRow, int numOfElems, std::string& element) {
	std::size_t found = 0;
	std::size_t lastFound = 0;
	int count = 0;
	element = "";

	if (csvRow.length() == 0) {
		return false;
	}
	// skip to the desired ,
	// have to count n+1, as n = the start of segment (except when n=0)
	for (count = 0; ((count <= numOfElems) && (found != std::string::npos)); ++count) {
		if (count > 0) {
			lastFound = found;
			found = csvRow.find(",", found + 1); // find the next , from the char after the last found one
		}
		else
		{
			found = csvRow.find(","); // find first ,
		}
	}

	// if a , found
	if (found != std::string::npos) {
		if (found == 0) {
			element = "";
			return true;
		}
		else {
			if (count == 1) {
				element = csvRow.substr(0, found);
			}
			else {
				element = csvRow.substr(lastFound + 1, (found - lastFound) - 1);
			}
			return true;
		}
	}

	// did we ask for a count too high?
	if (count == numOfElems) {
		return false;
	}
	// likely this is the last elem
	if (count == 0) {
		element = csvRow;
	}
	else {
		element = csvRow.substr(lastFound + 1);
	}

	return true;
}


void StripQuotesInner(std::string& valToClean, std::string findString) {
	// TODO: Keep Quotes for string with spaces
	std::size_t found = 0;

	do {


		// find next "findString"
		if (found > 0) {
			found = valToClean.find(findString, found + 1);
		}
		else {
			// find first
			found = valToClean.find(findString);
		}
		
		// is there anything to do?
		if (found != std::string::npos) {
			bool keepQuote = false;

			// check if there is a terminating string with spaces inbetween
			std::size_t foundNext = valToClean.find(findString, found + 1);

			if (foundNext != std::string::npos) {
				// there is a terminating quote, see if there's a space in between
				std::size_t foundSpace = valToClean.find(" ", found + 1);
				if ((foundSpace != std::string::npos) && (foundSpace < foundNext)) {
					// there is a space, keep the strings, and just skip ahead
					found = foundNext;
					keepQuote = true;
				}
			}

			// want to remove this (it's not encapsulating a space)
			if (keepQuote == false) {
				if (found == 0) {
					valToClean = valToClean.substr(1);
				}
				else {
					valToClean = valToClean.substr(0, found) + valToClean.substr(found + 1);
				}
			}
		}
	} while (found != std::string::npos);
}

std::string StripQuotesString(std::string& inputToClean) {
	std::string retVal = inputToClean;
	if (retVal.length() == 0) {
		return retVal;
	}

	StripQuotesInner(retVal, "\"");
	StripQuotesInner(retVal, "'");
	return retVal;
}

/*std::string StripQuotesChar(char* inputToClean) {

	if (inputToClean == nullptr) {
		return "";
	}
	std::string toPassVal(inputToClean);
	return StripQuotesString(toPassVal);
}*/

void LoadColumnNames(std::string headerRow, std::vector<std::string>& columnInfo) {
	std::string colName = "";
	bool keepAlive = true;

	while (keepAlive) {
		keepAlive = FindAndSplitNextCSVElement(headerRow, colName);
		columnInfo.push_back(StripQuotesString(colName));
	}

}

bool Is_number(const std::string& searchStr)
{
	// TODO: might have to re-write in non C++11 style if using GPU
	return (!searchStr.empty() &&
		std::find_if(searchStr.begin(), searchStr.end(), [](char chrToChk) {
		return (!std::isdigit(chrToChk)) &&
			(!(chrToChk == '-')) &&
			(!(chrToChk == '.')) &&
			(!(chrToChk == 'e')); }
	) == searchStr.end());
}