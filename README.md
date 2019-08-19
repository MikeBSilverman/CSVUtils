# Introduction 
Command line utilities to aid in preparing data sets for AI/ML training, specifically large CSVs.  
These tools keep a low memory footprint, regardless of the file size.  They're limited typically by Disk I/O and the # of processors.

# Background
I'll be honest, I studied C++ programming many many years ago.  Got back into it as I kept running into issues with  ML/AI experiments, and Excel and other tools just weren't working on my 16GB RAM laptop, or were single-threaded.  
I knew a better way was needed, so I built it myself.  
I'm rusty, so my coding may not be awesome.  Guilty as charged, always willing to learn to improve.

# Utilities
Look at the specific README.md file for each utility, for command line, parameters, etc.

CSVSplit - help filter CSVs files, or split CSVs based on simple conditions/logic.  (E.g. if MonthCol > 6.)  Or split randomly 80/20.  
CSVUnitTest - get simple statistics on the data within the CSV, quick summary to see if it's fit for ML/AI training.  Flag any errors easily.

# Build and Test
Coded using Visual Studio 2017, with either x86 or x64 mode.  (Disable precompiled headers)  
I run CPPCheck for coding issues.  

# Contribute
Please post issues, submit fixes, and offer up feature requests.