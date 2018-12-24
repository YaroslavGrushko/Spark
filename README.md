# Spark
hello, for example we have taxi service with 3000 taxi drivers, also we have 5000 clients of this service. Clients live in London and we know it's addresses. So,  
1. let's generate Big Data file of random orders of the clients to this service with some metadata (like start_point, end_point, start_date, end_date, etc.)  
2. than let's analyze this Big Data with Spark.  

# Prerequests
1. IDE that supports python (exm. Visual Studio Code - free)
2. Python installed  
3. Spark installed  
  a. download Spark from official website, create folder on your local drive (exm. *C:\Spark\\*) and put downloaded file to it (exm. *C:\Spark\spark-2.2.1-bin-hadoop2.6*)  
  b. download winutils for Windows 10 and put it to (*C:\Spark\winutils\\*)
  c. (for Windows 10) Settings > Edit environment variables:
    **SPARK_HOME=C:\Spark\spark-2.2.1-bin-hadoop2.6
    PYTHONPATH=%SPARK_HOME%\python;%PYTHONPATH%
    HADOOP_HOME=C:\Spark\winutils\*
# Get Started
  
 1. download this project 
 2. go to BigData folder and launch Big Data (you can set number of lines at  the bottom of .py the file)
 3. you can multiply the big date created in the previous step, go to DoubleBigData folder and launch .py program (you can set numbers of copies at the bottom of .py file)
 4. to analyze (with Spark) Big Data genereted on previous step launch .py program that located in Spark folder
 
