# New_york_yellow_taxi_Big-data
Analyzed New York City's Yellow taxi data set with Hadoop MapReduce
Link to download the dataset: https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv
TO explore other data sets for NYC taxis follow the link: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Initial Questions that I wanted to answer:
1. What is the average number of passengers per trip in general and per day of the week?
2. What is the average trip distance in general and per day of the week?
3. What are the most used payment types?
4. How does the number of Passengers change over the week day and weekend. A graph (using the output of a MapReduce job) showing the average number of passengers over the day (per hour).
5. How does the total distance travelled change over different time of the day for all days of the week. a graph showing the average trip distance over the day (per hour).

## Steps to execute the programs
- Individual java programs have been created to answer each of the Intial questions that were set up for analysis. The naming convention followed is QuestionX.java where x denotes the question number 
	for example; question 1 is solved in Question1.java
- The procedure of execution is as follows:
#### step 1: sudo docker exec -it namenode bash
#### step 2: export HADOOP_CLASSPATH=/usr/lib/jvm/java-1.8.0-openjdk-amd64/lib/tools.jar  
#### step 3: hadoop com.sun.tools.javac.Main QuestionX.java ( x denotes question number)
#### step 4: jar cf wc.jar QuestionX*.class
#### step 5:  hadoop jar wc.jar QuestionX /dataset /outputX
#### step 6: hdfs dfs -cat /outputX/part-r*

## Insights:

The graph can be found in the specific folder in the repository and depicts the variation in the number of passengers traveling in the taxi at the various instance of time during the day. The graph was drawn using Tableau from the output obtained using the map-reduce task. It can be inferred from the graph above that though both the lines follow the same trend i.e. the highs and the lows follow a similar pattern it is visible that the Average passengers traveling by taxi at any given time on a weekend are always higher than the corresponding value on weekdays.

The second graph epicts the variation in the number of passengers traveling in the taxi at the various instance of time during
the day. The graph was drawn using Tableau from the output obtained using the map-reduce task. It can be inferred from the graph
above that though both the lines follow the same trend i.e. the highs and the lows follow a similar pattern it is visible that the Average passengers traveling by taxi at any given time on a weekend are always higher than the corresponding value on weekdays.

* The graphs were plotted using Tableau and data from the same was obtained from the output of question 4 and question 5.
