import sys

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Usage: wordcount <input>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc = SparkContext(conf=conf)

    ageData = sc.textFile(sys.argv[1]).map(lambda x: x.split(" ")[1])
    totalAge = ageData.map(lambda age: int(age)).reduce(lambda a, b: a+b)
    counts = ageData.count()
    avgAge = totalAge / counts

    print("总人数", counts)
    print("年龄综合", totalAge)
    print("平均年龄", avgAge)

    sc.stop()
