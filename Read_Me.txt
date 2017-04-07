1. Create input and output location in HDFS

$ sudo su hdfs
$ hadoop fs -mkdir /user/cloudera
$ hadoop fs -chown cloudera /user/cloudera
$ exit
$ sudo su cloudera
$ hadoop fs -mkdir /user/cloudera/wordcount /user/cloudera/wordcount/input 

2. Move the input files to the input directory /user/cloudera/wordcount/input 

hadoop fs -put alice29.txt asyoulik.txt cp.html fields.c grammar.lsp lcet10.txt plrabn12.txt xargs.1 /user/cloudera/wordcount/input

3. Compile the DocWordCount program
$ mkdir -p build
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build -Xlint 

4. Create the jar file 
$ jar -cvf cloud.jar -C build/ . 

5. Run the DocWordCount application from the JAR file, passing paths to the input and output directories 
$ hadoop jar cloud.jar org.myorg.DocWordCount /user/cloudera/wordcount/input /user/cloudera/wordcount/output 

6. To view the ouput following line should be executed 
$ hadoop fs -cat /user/cloudera/wordcount/output/*

7. For moving the output into the text file following line of code should be executed
$ hadoop fs -ls /user/cloudera/wordcount/output/* 
$ hadoop fs -get /user/cloudera/wordcount/output/part-r-00000 DocWordCount.out


8. Delete the output files from the output directory
$ hadoop fs -rm -r /user/cloudera/wordcount/output

9. Term Frequency Execution 
Run the TermFrequency application from the JAR file, passing paths to the input and output directories 
$ hadoop jar cloud.jar org.myorg.TermFrequency /user/cloudera/wordcount/input /user/cloudera/wordcount/output 

10. To view the ouput following line should be executed 
$ hadoop fs -cat /user/cloudera/wordcount/output/*

11. For moving the output into the text file following line of code should be executed
$ hadoop fs -ls /user/cloudera/wordcount/output/* 
$ hadoop fs -get /user/cloudera/wordcount/output/part-r-00000 TermFrequency.out

12. Delete the output files from the output directory
$ hadoop fs -rm -r /user/cloudera/wordcount/output

13. TFIDF execution: Run the TFIDF application from the JAR file, passing paths to the input, intermidiate output and final output directories 
hadoop jar cloud.jar org.myorg.TFIDF /user/cloudera/wordcount/input /user/cloudera/wordcount/mid1 /user/cloudera/wordcount/tfidf11

For final output 
$ hadoop fs -ls /user/cloudera/wordcount/tfidf11/* 
$ hadoop fs -get /user/cloudera/wordcount/tfidf11/part-r-00000 TFIDF.out

14. Delete the files output directory 
hadoop fs -rm -r /user/cloudera/wordcount/output

15. Search Execution for query 1: Run the Search application from the JAR file and give the required query "computer science"
$ hadoop jar cloud.jar org.myorg.Search /user/cloudera/wordcount/tfidf11/* /user/cloudera/wordcount/output 

16. To view the ouput following line should be executed 
$ hadoop fs -cat /user/cloudera/wordcount/output/* 

17. For moving the output into the text file following line of code should be executed
$ hadoop fs -ls /user/cloudera/wordcount/output/* 
$ hadoop fs -get /user/cloudera/wordcount/output/part-r-00000 query1.out 

18. Rank Execution for query 1: Run the Rank application from the JAR file 
$ hadoop jar cloud.jar org.myorg.Rank /user/cloudera/wordcount/output/* /user/cloudera/wordcount/output2

19. For moving the output into the text file following line of code should be executed
$ hadoop fs -ls /user/cloudera/wordcount/output2/* 
$ hadoop fs -get /user/cloudera/wordcount/output2/part-r-00000 query1-rank.out 

20. Search execution for query 2: Execute the Search program again and give the required query "data analysis"
hadoop jar cloud.jar org.myorg.Search /user/cloudera/wordcount/tfidf11/* /user/cloudera/wordcount/output4 

21. For moving the output into the text file following line of code should be executed
$ hadoop fs -ls /user/cloudera/wordcount/output4/* 
$ hadoop fs -get /user/cloudera/wordcount/output4/part-r-00000 query2.out 

22. Rank Execution for query2: Run the Rank application from the JAR file 
$ hadoop jar cloud.jar org.myorg.Rank /user/cloudera/wordcount/output4/* /user/cloudera/wordcount/output8


23. For moving the output into the text file following line of code should be executed
$ hadoop fs -ls /user/cloudera/wordcount/output8/* 
$ hadoop fs -get /user/cloudera/wordcount/output8/part-r-00000 query2-rank.out
