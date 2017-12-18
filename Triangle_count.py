from pyspark import SparkConf, SparkContext
import sys
import re

# This script takes two arguments, an input and output
if len(sys.argv) != 3:
  print('Usage: ' + sys.argv[0] + ' <in> <out>')
  sys.exit(1)

input_location = sys.argv[1]
output_location = sys.argv[2]

# Set up the configuration and job context
conf = SparkConf().setAppName('Triangle')
sc = SparkContext(conf=conf)

data = sc.textFile(input_location)

# Getting all number from string to int
number = re.compile(r"[\d']+")
data = data.map(lambda n: number.findall(n))
data = data.map(lambda x: s[int(t) for t in x])

# Data Star
# Data Star means that there is an edge from first element to the second element
data_modified_star = data.map(lambda x: [((x[0], x[i]), '*') for i in range(1, len(x))]).flatMap(lambda x: x)

# Data Dash
# Data Dash means that there exists a edge from the first element to the second element
data_modified_dash = data.map(lambda x: [[((x[j], x[i]), x[0]) for j in range(1, i)] for i in range(1, len(x))])
data_modified_dash = data_modified_dash.map(lambda x: reduce(lambda a, b: a + b, x, [])).flatMap(lambda x: x)

# RDD join long data
# Joint by key
data_merge = data_modified_dash.join(data_modified_star)

# eliminate multiplication
result = data_merge.map(lambda x: tuple(sorted(x[0]+x[1])[0:3])).distinct()

# save output data
result.saveAsTextFile(output_location)

sc.stop()