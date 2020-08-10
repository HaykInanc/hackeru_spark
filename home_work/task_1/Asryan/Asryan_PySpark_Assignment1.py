# Import the required libraries
from pyspark import SparkContext, SparkConf
import collections
from operator import add

if __name__ == "__main__":

	# Set PySpark configurations
    conf = SparkConf().setAppName("unionNasaLogs").setMaster("local[*]")

    # Create a PySpark context
    sc = SparkContext(conf = conf)

    # Import the logs for July
    julLogs = sc.textFile("../../in/nasa_19950701.tsv")

    # Import the logs for August
    augLogs = sc.textFile("../../in/nasa_19950801.tsv")
    
    # Aggragate two logs into a single RDD (union) and remove the header row
    aggregateLogs = julLogs.union(augLogs).filter(lambda line: not line.startswith("host"))    
    
    # Extract the websites into an RDD
    websites = aggregateLogs.map(lambda line: line.split()[4])


    ####### Approach #1: Result = RDD #######
    # Use .map to add a value of 1 for each RDD row, then reduce by key (website)
    result1 = websites.map(lambda x: (str(x),1)).reduceByKey(add)

    # Save the resulting RDD to a .text file
    result1.saveAsTextFile('out/Asryan_HW1.text')

    ####### Approach #1: Result = RDD #######
    # Return the count of each unique value in the RDD as a dictionary of (value, count) pairs.
    result2 = websites.countByValue()
    
    # Use the collections library to sort the resulting dictionary
    sortedResults = collections.OrderedDict(sorted(result2.items()))
	
	# Print the result
    for key, value in sortedResults.items():
    	print("%s %i" % (key, value))

    # Write to .txt
    with open('out/Asryan_HW1.txt', 'w') as file:
    	for p in sortedResults.items():
    		file.write("%s,%s\n" % p)