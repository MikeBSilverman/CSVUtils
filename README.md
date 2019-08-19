# Introduction 
Command line utilities to aid in preparing data sets for AI/ML training, specifically large CSVs.  
These tools keep a low memory footprint, regardless of the file size.  They're limited typically by Disk I/O and the # of processors.

CSVSplit - help filter CSVs files, or split CSVs based on simple conditions/logic.  (E.g. if MonthCol > 6.)  Or split randomly 80/20.  
CSVUnitTest - get simple statistics on the data within the CSV, quick summary to see if it's fit for ML/AI training.  
- Any columns with the same value throughout  
- Quick, simple stats on each column (check for bias - pct of male vs. female for example)

# Build and Test
Coded using Visual Studio 2017, with either x86 or x64 mode.  (Disable precompiled headers)

# Contribute
