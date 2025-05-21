# Task 1 - Vinicius Pinho and Gabriel Vaz Cançado Ferreira

from pyspark import SparkContext
import random


print('Task 1 \n')
'''
 (a) Start by creating a list of 0 to 40000, afterwards use parallelize() to make an RDD
 from the list (distribute your list into four partitions). Use RRD’s filter and a
 lambda function to convert your list into a list of odd numbers. Show the result by
 using the take() function and output the first seven numbers in the odds list. 
'''
print('a) \n')
sc = SparkContext("local[*]", "a")

numbers_list = list(range(0,40001))

RDD = sc.parallelize(numbers_list,4)

odd_numbers_list =  RDD.filter(lambda x: x % 2 != 0)

print(odd_numbers_list.take(7))

print('\n')

sc.stop()

print('End (a) \n')
# ==========================================================================================

'''
 (b) Create an RDD with 10,000 random integers between 1 and 100. Partition the RDD
 into 5 partitions. Count how many numbers fall into each of the following buckets:
 0-20, 21-40, 41-60, 61-80, 81-100 Show the results as key-value pairs: (”1-20”, count),
 (”21-40”, count), ...
'''

print('b) \n')

sc = SparkContext("local[*]", "b")

random_list = [random.randint(1, 100) for _ in range(10000)]

rdd = sc.parallelize(random_list, 5)

def assign_bucket(n): # chatgpt was used to create this function given the first two if conditions inputed 
    if 1 <= n <= 20:
        return ("1-20", 1)
    elif 21 <= n <= 40:
        return ("21-40", 1)
    elif 41 <= n <= 60:
        return ("41-60", 1)
    elif 61 <= n <= 80:
        return ("61-80", 1)
    elif 81 <= n <= 100:
        return ("81-100", 1)

bucket_counts = rdd.map(assign_bucket).reduceByKey(lambda a, b: a + b)

results = bucket_counts.collect()
print(results)

print('\n')

sc.stop()

print('End (b) \n')

# ==========================================================================================

'''
 (c) Explain what the parallelize() function does. Define RDD and describe the process
 used step-by-step in part (a). After that, compare this approach with filtering even
 numbers in a list using Python’s built-in filter() function (as done in the previous
 assignment). Highlight the differences between using RDD’s filter() and Python’s
 filter() in terms of execution and scalability.
'''

# What does the parallelize() function does? 

'''
This function is used to separate a python collection like a list
across a Spark cluster, creating an RDD, resilient distributed dataset.
It allows Spark to operate in parallel, splitting it into partitions
that can be processed concurrently. 
'''

# Define RDD

'''
The resilient distributed dataset is a data structure in Spark that
is immutable, distributed, fault-tolerant and support parallel operations.
'''

# Describe the process used step-by-step in part (a)

'''
i) Created a list of integers from 0 to 40000:
numbers_list = list(range(0,40001))

ii) Used the parallelize() function to convert the list into an RDD with 4 partitions:
RDD = sc.parallelize(numbers_list,4)

iii) Applied the filter() function to keep only odd numbers:
odd_numbers_list =  RDD.filter(lambda x: x % 2 != 0)

iv) Use take(7) to print the first 7 odd numbers:
print(odd_numbers_list.take(7))
'''

# compare this approach with filtering even
# numbers in a list using Python’s built-in filter() function (as done in the previous
# assignment). Highlight the differences between using RDD’s filter() and Python’s
# filter() in terms of execution and scalability.

'''
The approach using PySpark RDD´s filter() has many advantages over pythons´s 
built-in filter(). In terms of execution, the RDD filter() is distributed, i.e.
runs in multiple cores meanwhile python´s filter() runs on a single core. In terms 
of scalability, RDD´s filter() is highly scalable for big data meanwhile python´s
filter() is limited to memory and CPU of one machine. On the other hand,
pythons´s filter() is faster for small data since it has low overhead. 
'''

# ==========================================================================================

'''
(d) Count the number of appearances of each word in the text file ”Text-Data” using
PySpark. Show the results of the 25 words with the most occurrences as a tuple
(e.g., (”word”, 23)).
'''
print('d) \n')

sc = SparkContext("local[*]", "d")

rdd = sc.textFile("/mnt/c/Users/gabri/Clausthal/Documents/Study/BigData/Big-Data-Management/Assignment2/Text-Data.txt")

words = rdd.flatMap(lambda line: line.split())

word_pairs = words.map(lambda word: (word.lower(), 1))

word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

sorted_counts = word_counts.sortBy(lambda pair: pair[1], ascending=False)

top_25 = sorted_counts.take(25)

for word, count in top_25:
    print((word, count))

sc.stop()

print('\n')

print('End (d) \n')

# ==========================================================================================

'''
(e) List all those words, which can be classified as ”auxiliary verbs” or ”function words”
 to exclude them from your results (e.g. ”by”, ”to”, ”and” are ”not-meaningful
words”). Order each ”meaningful” word by occurrence.
'''

print('e) \n')

sc = SparkContext("local[*]", "e")

stop_words = set([
    "a", "an", "the", "and", "or", "but", "if", "in", "on", "at", "to", "of", "for", "with", "by",
    "is", "am", "are", "was", "were", "be", "been", "being", "have", "has", "had", "do", "does", "did",
    "will", "would", "shall", "should", "can", "could", "may", "might", "must", "that", "this", "these", "those"
]) # Chatgpt was used to create this list of words

rdd = sc.textFile("/mnt/c/Users/gabri/Clausthal/Documents/Study/BigData/Big-Data-Management/Assignment2/Text-Data.txt")

word_counts = (
    rdd.flatMap(lambda line: line.lower().split())          # separa palavras
       .filter(lambda word: word.isalpha())                 # mantém só palavras (sem números/pontuação)
       .filter(lambda word: word not in stop_words)         # remove stop words
       .map(lambda word: (word, 1))                         # cria pares (palavra, 1)
       .reduceByKey(lambda a, b: a + b)                     # soma as contagens
       .sortBy(lambda x: x[1], ascending=False)             # ordena por contagem descrescente
)

top25 = word_counts.take(25)

for word, count in top25:
    print((word, count))

print('\n')

print('End (e) \n')

# ==========================================================================================

print('End task 1 \n')