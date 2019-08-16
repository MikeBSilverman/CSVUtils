#pragma once
//#include "stdafx.h"
#include <string>
#include <map>

bool FindAndSplitNextCSVElement(std::string&, std::string&);
bool FindASpecificCSVElement(std::string&, int, std::string&);
void LoadColumnNames(std::string, std::vector<std::string>&);

std::string StripQuotesString(std::string&); 
//std::string StripQuotesChar(char*);
void StripQuotesInner(std::string&, std::string);

bool Is_number(const std::string&);



template <class K, class V>
class map_second_value_equals {
private:
	V value;
public:
	// constructor
	explicit map_second_value_equals(const V& v) : value(v) {}
	
	// comparison
	bool operator() (std::pair<const K, V> elem) {
		return elem.second == value;
	}
};