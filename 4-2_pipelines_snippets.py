"""
Code fragments from Lesson 4.2, `Pipelines and cacheing'
Note: the function isprime is not defined here, but you can
get a similar function via the week 2 resources.
"""

# make an RDD of a million elements, filtering out the prime numbers and counting them
sc.range(1000000).filter(isprime).count()
# make an RDD of primes
primes = sc.range(1000000).filter(isprime)
# take a sample of 10 elements from it
primes.takeSample(True, 10)
# tell Spark engine you are going to reuse an RDD using cache()
primes.cache()
# take a sample
primes.takeSample(True, 10)
# repeating the sampling process is faster the second time...
primes.takeSample(True, 10)
# if RDD very large, cache would exhaust memory
# instead use persist() to persist RDD to disk
# first redefine RDD as Spark doesn't let you change cacheing of an RDD
primes = sc.range(1000000).filter(isprime)
primes.persist(StorageLevel.DISK_ONLY) # use MEMORY_AND_DISK if you aren't sure of RDD's size
