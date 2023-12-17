import pyspark
sc = pyspark.SparkContext('local[*]')

nums = sc.parallelize([1, 2, 3])
squares = nums.map(lambda x: x*x) 
even = squares.filter(lambda x: x % 2 == 0) 
even.collect()
x = sc.parallelize(["spark rdd example", "sample example"])
y = x.flatMap(lambda x: x.split(' ')) 

print("output:",y.collect())