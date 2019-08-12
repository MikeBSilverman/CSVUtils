#include "stdafx.h"
#include "CSVFilter.h"
#include "CLParams.h"
#include <iostream>
#include <map>

void InitializeFilterOperandsEnum(filterOpMap& mapFilterOpValues)
{
	mapFilterOpValues["le"] = filterOpLE;
	mapFilterOpValues["lt"] = filterOpLT;
	mapFilterOpValues["ge"] = filterOpGE;
	mapFilterOpValues["gt"] = filterOpGT;
	mapFilterOpValues["eq"] = filterOpEQ;
	mapFilterOpValues["ne"] = filterOpNE;
}

void InitializeFilterJoinOperandsEnum(filterJoinOpMap& mapFilterJoinOpValues) {
	mapFilterJoinOpValues["AND"] = filterJoinOpAND;
	mapFilterJoinOpValues["OR"] = filterJoinOpOR;
}


int LoadFilters(filterParamVectorType& filterInfo, inputParamVectorType& inputParameters, std::vector<std::string>& columnInfo) {
	int i = 1;
	filterParam thisFilter;
	std::string filterName = "";
	do {
		filterName = "-filter" + std::to_string(i);
		thisFilter = FindFilter(filterName, inputParameters, columnInfo);
		if (thisFilter.op == "ERROR") {
			std::cerr << "Invalid column name in " << "-filter" + std::to_string(i) << std::endl;
			return 5;
		}
		if (thisFilter.variable.length() > 0) {
			filterInfo.push_back(thisFilter);
		}
		++i;
	} while (thisFilter.variable.length() > 0);
	return 0;
}


filterParam FindFilter(std::string& param, inputParamVectorType& inputParameters, std::vector<std::string>& columnInfo) {
	filterParam retVal;
	unsigned int i = 0;

	while (i < inputParameters.size()) {
		if ((param == inputParameters[i]) && ((i + 3) < inputParameters.size())) {
			if (inputParameters[i + 3].length() > 0) {
				// Find the column #
				unsigned int j = 0;
				while ((j < columnInfo.size()) && (columnInfo[j] != inputParameters[i + 1])) {
					++j;
				}
				if (j == columnInfo.size()) {
					retVal.op = "ERROR";
					return retVal; // return error, something is wrong
				}
				retVal.colNum = j;
				retVal.variable = inputParameters[i + 1];
				retVal.op = inputParameters[i + 2];
				retVal.value = inputParameters[i + 3];

				// Check if a join
				if ((i + 4) < inputParameters.size()) {
					if (("AND" == inputParameters[i + 4]) || ("OR" == inputParameters[i + 4])) {
						retVal.joinToNextFilter = inputParameters[i + 4];
					}
				}

				return retVal;
			}
			else {
				return retVal;
			}
		}
		else {
			++i;
		}
	}
	return retVal;
}

filterOperands GetFilterOperand(std::string& operand, filterOpMap& mapFilterOpValues) {
	return mapFilterOpValues[operand];
}
filterJoinOperands GetFilterJoinOp(std::string& joinOp, filterJoinOpMap& mapFilterJoinOpValues) {
	return mapFilterJoinOpValues[joinOp];
}