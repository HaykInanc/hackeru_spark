# Python Spec
# Lesson 10 
# HW 3
# E.Chumbaeva
# 2020-08-10
#  
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("word count").setMaster("local[3]")
    sc = SparkContext(conf = conf)
    
    lines = sc.textFile("../in/word_count.text")
    
    words = lines.flatMap(lambda line: line.split(" ")) \
    			.filter(lambda val: len(val)>5)
    
    wordCounts = words.countByValue()
    
    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))



	conf = SparkConf().setAppName('nasa_requests').setMaster('local[*]')
	sc = SparkContext(conf = conf)


	julyLogs =  sc.textFile('../../in/nasa_19950701.tsv')
	augustLogs =  sc.textFile('../../in/nasa_19950801.tsv')

	aggregateLogs = julyLogs.union(augustLogs)
	aggregateLogs = aggregateLogs.filter(lambda line: not line.startswith('host')) # Убираем заголовки

	addresses = lines.flatMap(lambda line: line_split("\t"))

	addressesCounts = addresses.countByValue()

	for address, count in addressesCounts.items():
		print("{} : {}".format(word, count))
