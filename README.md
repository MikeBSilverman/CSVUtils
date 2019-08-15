# Introduction 
Command line utilities to aid in preparing data sets for AI/ML training, specifically large CSVs.
Note: CSVMerge is in progress

CSVSplit - help filter CSVs files, or split CSVs based on simple conditions/logic.  (E.g. if MonthCol > 6.)  Or split randomly 80/20.  
CSVUnitTest - get simple statistics on the data within the CSV, quick summary to see if it's fit for ML/AI training.  
- Any columns with the same value throughout  
- Quick, simple stats on each column (check for bias - pct of male vs. female for example)

# CSVSplit Command Line Args
- inputf "file name of data to analyze" (Required)
- outputf "file name of primary output - if filters = true" (Required)
- outputfother "file name of other output - if filters = false" (optional for when splitting files)
- processqueuebuffer # of bytes to use for input buffer (default = 1000000000)  
- coltoremove# or coltokeep# positive or negative list of column names to keep/remove  

Can then use filter OR percentagesplit, but not both together:
- filter#  
    - "Variable to filter on" (Required)   
	- operand (eq, ne, lt, le, gt, ge) (Required)  
	- value to search on (Required)  
	- join operand (AND, OR) (Required for all filters up to n-1)  
    - e.g. -filter1 Year ge 2009 AND -filter2 Year le 2014  
- percentagesplit .xx  - e.g. if .80 then 80% goes into normal file, remainder 20% will go into other file  
  
# Build and Test
Coded using Visual Studio 2017, with either x86 or x64 mode.

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
