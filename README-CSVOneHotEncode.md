# Introduction 
CSVOneHotEncode - enumerate the values in a single column into boolean into individual columns.  Multi-threaded analysis.   

# Intended Use Cases
- E.g. If you have male/female in one column, 1>0 and that may confuse an AI model.  Instead you should have col.male (true/false) and col.female (true/false).  If col.female = true, col.male must be false.  
This helps eliminate bias in models.
All while keeping a low memory profile.  (The biggest factor in performance is Disk I/O)

# CSVUnitTest Command Line Args
- inputf "file name of data to analyze" (Required)
- outputf "file name of output of statistical analysis" (Required) will be CSV output
- colToEnc "name of column to encode" (Required)
- removeOld remove the original column to encode (optional)

# Example
.\CSVOneHotEncode.exe -inputf "C:\temp\TestData.csv" -outputf "C:\temp\outputstat.csv" -colToEnc FieldToEncode -removeOld
  
# Build and Test
Coded using Visual Studio 2017, with either x86 or x64 mode.  (Disable precompiled headers)

# Contribute
Please post issues, submit fixes, and offer up feature requests.