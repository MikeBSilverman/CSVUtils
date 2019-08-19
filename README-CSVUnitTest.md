# Introduction 
CSVUnitTest - get simple statistics on the data within the CSV, quick summary to see if it's fit for ML/AI training.
Multi-threaded analysis.   
Flag any errors easily (output is a CSV which can get ingested into other tools).  

# Intended Use Cases
- Are any columns leading indicators for the label column?  (Did you perhaps leave some working columns in the dataset?  I've done it before...)
- Any columns with the same value throughout (is a column all 0s or 1s?  Why use that as an input to an ML engine if so?)  
- Check for bias - pct of male vs. female for example.  Is there, let's say >50% more females than males in this dataset?
- Quick, simple stats on each column 

All while keeping a low memory profile.  (The biggest factor in performance is Disk I/O)


# CSVUnitTest Command Line Args
- inputf "file name of data to analyze" (Required)  
- outputf "file name of output of statistical analysis" (Required) will be CSV output  
- labelCol "name of column with the expected output of the model, for comparison" (optional)  
  
# Build and Test
Coded using Visual Studio 2017, with either x86 or x64 mode.  (Disable precompiled headers)

# Contribute
Please post issues, submit fixes, and offer up feature requests.