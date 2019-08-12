#pragma once
#include <vector>
#include <string>
#include <map>
#include "CLParams.h"

struct filterParam
{
	std::string variable = "";
	std::string value = "";
	std::string op = "";
	std::string joinToNextFilter = "";
	unsigned int colNum = 0;
};

enum filterOperands {
	filterOpNotDefined,
	filterOpLE,
	filterOpLT,
	filterOpGE,
	filterOpGT,
	filterOpEQ,
	filterOpNE
};
enum filterJoinOperands {
	filterJoinOpNotDefined,
	filterJoinOpAND,
	filterJoinOpOR
};

typedef std::vector<filterParam> filterParamVectorType;
typedef std::map<std::string, filterOperands> filterOpMap;
typedef std::map<std::string, filterJoinOperands> filterJoinOpMap;

void InitializeFilterOperandsEnum(filterOpMap&);
void InitializeFilterJoinOperandsEnum(filterJoinOpMap&);
filterParam FindFilter(std::string&, inputParamVectorType&, std::vector<std::string>&);
int LoadFilters(filterParamVectorType&, inputParamVectorType&, std::vector<std::string>&);
filterOperands GetFilterOperand(std::string&, filterOpMap&);
filterJoinOperands GetFilterJoinOp(std::string&, filterJoinOpMap&);
