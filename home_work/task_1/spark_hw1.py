# Python Spec
# Lesson 10 
# HW 3
# E.Chumbaeva
# 2020-08-10
# https://github.com/HaykInanc/hackeru_spark/pull/2
from pyspark import SparkContext, SparkConf
import io

if __name__ == "__main__":

	def getResultLine(address, count):
		return ' : '.join(address, count)



	conf = SparkConf().setAppName('nasa_requests').setMaster('local[*]')
	sc = SparkContext(conf = conf)


	julyLogs =  sc.textFile('../../in/nasa_19950701.tsv')
	augustLogs =  sc.textFile('../../in/nasa_19950801.tsv')

	aggregateLogs = julyLogs.union(augustLogs)
	aggregateLogs = aggregateLogs.filter(lambda line: not line.startswith('host')) # Убираем заголовки

	print('*' * 20)

	addresses = aggregateLogs.map(lambda line: line.split("\t")[0])
	addressesCounts = addresses.countByValue()

	f = open('spark_hw1.text', 'w', encoding='utf-8')
	for address, count in addressesCounts.items():
		f.write("{} : {}\n".format(address, count))
	f.close()


	#addressesCounts = addressesCounts.map(getResultLine(addressesCounts.items()))
	#addressesCounts.saveAsTextFile('../../out/spark_hw1.text')

	print('*' * 20)
