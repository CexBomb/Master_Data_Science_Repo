paragraphs = sc.newAPIHadoopFile('data/shakespeare.txt', "org.apache.hadoop.mapreduce.lib.input.TextInputFormat","org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text",conf={"textinputformat.record.delimiter": '\n\n'}).map(lambda l:l[1])

# Me cargo todo lo que no sea una letra o numero
cleanParagraphs = paragraphs.map(lambda paragraph: re.sub('[^a-zA-Z0-9 ]','',paragraph.lower().strip()))

# Quito los parrafos vacíos
cleanParagraphs = cleanParagraphs.map(lambda paragraph: re.sub('[ ]+',' ',paragraph)).filter(lambda l: l!='')

cleanParagraphs.toDebugString() #Para ver el "linaje". Todo el camino que recorre el RDD. Se llama DAG. Esto permite recuperarlo en caso de caida

cleanParagraphs.getStorageLevel() #Muestra los niveles de almacenaje del RDD

# Ejercicio: contar el número de palabras de cada parrafo
cleanParagraphs.map(lambda x: len(x.split(' '))).take(5)

# Hacer histograma de lo anterior
tmp =  cleanParagraphs.map(lambda x: len(x.split(' '))).map(lambda num: (num,1))
tmp = tmp.reduceByKey(lambda x,y: x+y)

#Sacar la frecuencia de cada palabra
import numpy as np

from pyspark.mllib.feature import HashingTF

wordInDoc = cleanParagraphs.flatMap(lambda p: p.split(' ')).distinct().cache()
hashingTF = HashingTF(wordInDoc)
hashingTF = HashingTF(wordInDoc.count())

