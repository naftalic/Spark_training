#
pg = sc.wholeTextFiles("pg1000", 16)
pg.count()
pg.saveAsSequenceFile("pg1000.seq")

#
pg = sc.sequenceFile("pg1000.seq")
pg.count()

#
bv = sc.broadcast(range(1000))

#
sc.parallelize([1,2,3]).flatMap(lambda x: bv.value).count()
