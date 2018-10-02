from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("spark01")
    sc = SparkContext(conf = conf)

    def my_map():
        data = [1, 2, 3, 4, 5]
        rdd1 = sc.parallelize(data)
        rdd2 = rdd1.map(lambda x: x*2)
        print(rdd2.collect())

    def my_map2():
        a = sc.parallelize(["dog", "tiger", "lion", "cat", "panther"])
        b = a.map(lambda x: (x, 1))
        print(b.collect())

    def my_filter():
        data = [1, 2, 3, 4, 5]
        # rdd1 = sc.parallelize(data)
        # mapRdd = rdd1.map(lambda x: x*2)
        # filterRdd = mapRdd.filter(lambda x: x > 5)
        # print(filterRdd.collect())
        print(sc.parallelize(data).map(lambda x: x*2).filter(lambda x: x > 5).collect())

    def my_flatMap():
        data = ["hello spark", "hello world", "hello world"]
        rdd = sc.parallelize(data)
        print(rdd.flatMap(lambda line: line.split(" ")).collect())

    def my_groupBy():
        data = ["hello spark", "hello world", "hello world"]
        rdd = sc.parallelize(data)
        mapRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
        groupByRdd = mapRdd.groupByKey()
        print(groupByRdd.map(lambda x: {x[0]: list(x[1])}).collect())

    def my_reduceByKey():
        data = ["hello spark", "hello world", "hello world"]
        rdd = sc.parallelize(data)
        mapRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
        reduceByKeyRdd = mapRdd.reduceByKey(lambda a, b: a + b)
        print(reduceByKeyRdd.collect())

    def my_sortByKey():
        data = ["hello spark", "hello world", "hello world"]
        rdd = sc.parallelize(data)
        mapRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
        reduceByKeyRdd = mapRdd.reduceByKey(lambda a, b: a + b)
        sortByNumRdd = reduceByKeyRdd.map(lambda x: (x[1], x[0])).sortByKey(False).map(lambda x: (x[0], x[1]))
        print(sortByNumRdd.collect())

    def my_union():
        a = sc.parallelize([1, 2, 3])
        b = sc.parallelize([3, 4, 5])
        print(a.union(b).collect())

    def my_distinct():
        a = sc.parallelize([1, 2, 3])
        b = sc.parallelize([3, 4, 2])
        print(a.union(b).distinct().collect())

    def my_join():
        a = sc.parallelize([("A", "a1"), ("C", "c1"), ("D", "d1"), ("F", "f1"), ("F", "f2")])
        b = sc.parallelize([("A", "a2"), ("C", "c2"), ("C", "c3"), ("E", "e1")])
        # print(a.join(b).collect())  # inner join
        # print(a.leftOuterJoin(b).collect())  # outer join
        # print(a.rightOuterJoin(b).collect())
        print(a.fullOuterJoin(b).collect())

    def my_action():
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        rdd = sc.parallelize(data)
        print(rdd.count())
        print(rdd.take(3))
        print(rdd.max())
        print(rdd.min())
        print(rdd.sum())
        print(rdd.reduce(lambda x, y: x+y))
        print(rdd.foreach(lambda x: print(x)))

    # my_map()
    # my_map2()
    # my_filter()
    # my_flatMap()
    # my_groupBy()
    # my_reduceByKey()
    # my_sortByKey()
    # my_union()
    # my_distinct()
    # my_join()
    my_action()

    sc.stop()
