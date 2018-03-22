# Fragment 1

  import re
  texts = sc.wholeTextFiles("pg1000", 16)
  counts = texts.flatMap(lambda x: re.split(r'\W+',x[1])) \
                .map(lambda word: (word.lower(), 1)) \
                .reduceByKey(lambda x, y: x + y)
  results = count.collect()

# Fragment 2

  for (word, count) in sorted(results, key=lambda x: x[0])[0:30]
      print("%s: %i" % (word, count))

# Fragment 3

  for (word, count) in sorted(results, key=lambda x: -x[1])[0:30]
      print("%s: %i" % (word, count))


# Fragment 4

  import random
  def test(arg):
      x = random.random()
      y = random.random()
      return 0 if (x*x + y*y > 1) else 1

# Fragment 5

  sc.range(1000).map(test).mean() * 4.0
