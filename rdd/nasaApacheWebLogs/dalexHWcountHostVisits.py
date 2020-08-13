from pyspark import SparkContext, SparkConf
import itertools

conf = SparkConf().setAppName('NASA_count_host_visits').setMaster('local[*]')
sc = SparkContext(conf = conf)
def getHostCol(line):
	line_col = line.split('\t')
	return ','.join([line_col[0]])



julyLogs = sc.textFile('../../in/nasa_19950701.tsv')
augustLogs = sc.textFile('../../in/nasa_19950801.tsv')

nasaLogs = julyLogs.union(augustLogs).filter(lambda line: not line.startswith('host'))
#nasaLogs = (''.join(map(str, (list(nasaLogs.map(lambda line: getHostCol(line)).countByValue().items())))))
nasaLogs = (nasaLogs.map(lambda line: getHostCol(line)).countByValue().items())

nasaLogs = ('\n'.join([str(i) for i in itertools.chain(*nasaLogs)])).split('\n')
#nasaLogs = list(nasaLogs.map(lambda line: getHostCol(line)).countByValue().items())

sc.parallelize(nasaLogs).saveAsTextFile('../../out/NASA_count_host_visits.tsv')
#nasaLogs.saveAsTextFile('../../out/NASA_count_host_visits.tsv')
#print(nasaLogs)

# for host, count in nasaLogs:
# 	print("{} - {}".format(host, count))

