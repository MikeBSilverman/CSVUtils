#pragma once
//#include "stdafx.h"
#include <string>

bool FindAndSplitNextCSVElement(std::string&, std::string&);
bool FindASpecificCSVElement(std::string&, int, std::string&);

std::string StripQuotesString(std::string&); 
//std::string StripQuotesChar(char*);
void StripQuotesInner(std::string&, std::string);

bool Is_number(const std::string&);
