'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

import os


spark = SparkSession.builder.appName('Optimize I').getOrCreate()
base_path = os.getcwd()
project_path = ('\\').join(base_path.split('\\'))

answers_input_path = os.path.join(project_path, 'data\\answers')
file = os.listdir(answers_input_path)[0]
answersDF = spark.read.option('path', answers_input_path + '\\' + file).load()

questions_input_path = os.path.join(project_path, 'data\\questions')
file = os.listdir(questions_input_path)[0]
questionsDF = spark.read.option('path', questions_input_path + '\\' + file).load()

for file in os.listdir(answers_input_path)[1:]:
    if file[:2] != '_S':
        DF = spark.read.option('path', answers_input_path + '\\' + file).load()
        answersDF = answersDF.union(DF)
answersDF.show()
answersDF.count()

for file in os.listdir(questions_input_path)[1:]:
    if file[:2] != '_S':
        DF = spark.read.option('path', questions_input_path+ '\\' + file).load()
        questionsDF = questionsDF.union(DF)
questionsDF.show()

'''
Answers aggregation

Here we : get number of answers per question per month
'''

answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()

'''
Task:

see the query plan of the previous result and rewrite the query to optimize it
'''
answersDF.createOrReplaceTempView('answers')
questionsDF.createOrReplaceTempView('questions')
results = spark.sql('SELECT q.question_id, q.creation_date, q.title, a.month, a.cnt FROM ' +
                  '(SELECT question_id, month(creation_date) AS month, count(*) as cnt '+
                  'FROM answers '+
                  'GROUP BY question_id, month) as a ' +
                  'INNER JOIN ' +
                  'questions as q ' +
                  'ON ' +
                  'a.question_id=q.question_id ' +
                  'ORDER BY question_id').show()