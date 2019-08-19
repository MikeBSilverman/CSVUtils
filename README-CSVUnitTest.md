# Introduction 
CSVSplit - help filter CSVs files, or split CSVs based on simple conditions/logic.  (E.g. if MonthCol > 6.)  Or split randomly 80/20.  
I've found that Python or other tools are not great, especially when working with large #s of rows or columns, as you have your memory as a large constraint.  
(Microsoft R or RevoScaler is disk focused instead of memory, but it is typically single-threaded for many operations.)  

# Intended Use Cases
- keep or remove columns (E.g. trim the label column off of a large dataset)
- filter out NULLs or other bad data easily
- split the data 80/20 for training/test purposes

All while keeping a low memory profile.  (The biggest factor in performance is Disk I/O)


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
Coded using Visual Studio 2017, with either x86 or x64 mode.  (Disable precompiled headers)

# Contribute
Please post issues, submit fixes, and offer up feature requests.