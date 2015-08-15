# coding: utf-8

def parsePoint(line):
    """Converts a comma separated unicode string into a `LabeledPoint`.

    Args:
        line (unicode): Comma separated unicode string where the first element is the label and the
            remaining elements are features.

    Returns:
        LabeledPoint: The line is converted into a `LabeledPoint`, which consists of a label and
            features.
    """
    etiq = line.split(',')
    return LabeledPoint(etiq[0], etiq[1:])


import os.path
import numpy as np
from pyspark.mllib.regression import LabeledPoint
baseDir = os.path.join('data')
inputPath = os.path.join('mydata', 'matriz_reglas_trans_2.0.csv')
fileName = os.path.join(baseDir, inputPath)

reglas = sc.textFile(fileName, 8)
LPmoviesRDD = reglas.map(parsePoint).cache()


matrixRDD = LPmoviesRDD.map(lambda lp: lp.features)

mat = matrixRDD.collect()
matriz = np.matrix(mat)
Coomatriz = matriz * matriz.T
print Coomatriz


###Testeo de resultados

onlyLabels = LPmoviesRDD.map(lambda lp: lp.label)
mini = onlyLabels.take(1)[0]
maxi = onlyLabels.take(LPmoviesRDD.count())
print LPmoviesRDD.count(), mini, maxi
vector = Coomatriz[0]
vector2 = matriz[0]
print len(Coomatriz), vector.sum(), vector2.sum()
print Coomatriz[0]


##Unión de las id de películas con sus vectores de coocurrencia.
##NO USADA FINALMENTE

labelslist = onlyLabels.collect()
labels = np.array(labelslist)
Matriz = np.c_[labels, Coomatriz]


##Preparo los datos en túplas (movieId1, moveId2, peso) donde peso es es el cruce de las movieIds en la Coomatriz

#Túpla (movieId1, movieId2)
mov = onlyLabels.collect()
for x in range(len(mov)):
    mov[x] = int(mov[x])
movRDD = sc.parallelize(mov).zipWithIndex() 
resultRDD = movRDD.cartesian(movRDD)

#Filtro aquellas túplas con mismo movieID, y elimino duplicados
resultRDD = resultRDD.filter(lambda (x, y): x<y)

#túplas (movieId1, moveId2, peso)
Neo4jRDD = resultRDD.map(lambda ((a, b), (c, d)): (a, c, int(Coomatriz[b, d]))).filter(lambda (a, b, c): c <> 0)

#Grabar datos en CSV para importar en Neo4j
def toCSVLine(data):
    return ','.join(str(d) for d in data)  

lines = Neo4jRDD.map(toCSVLine)
lines.saveAsTextFile('data/mydata/Relaciones')


###Testeo de resultados

m = sc.parallelize([[0, 1, 2, 3], [4, 2, 6, 7], [8, 3, 7, 11]])
coo = sc.parallelize([[1, 2, 3], [2, 6, 7], [3, 7, 11]])
l = sc.parallelize([0, 4, 8])
print m.collect(), coo.collect(), l.collect()
coom = np.matrix(coo.collect()) 
print coom[0, 1]

#Túpla (movieId1, movieId2)
movtest = l.collect()
movtestRDD = sc.parallelize(movtest).zipWithIndex() 
resultestRDD = movtestRDD.cartesian(movtestRDD)
r = resultestRDD.collect()

#Filtro aquellas túplas con mismo movieID, y elimino duplicados
resultestRDD = resultestRDD.filter(lambda (x, y): x<y)

#túplas (movieId1, moveId2, peso)
Neo4jRDDtest = resultestRDD.map(lambda ((a, b), (c, d)): (a, c, coom[b, d])).collect()

print movtestRDD.collect()
print r, resultestRDD.collect()
print Neo4jRDDtest

## [[0, 1, 2, 3], [4, 2, 6, 7], [8, 3, 7, 11]] [[1, 2, 3], [2, 6, 7], [3, 7, 11]] [0, 4, 8]
## 2
## [(0, 0), (4, 1), (8, 2)]
## [((0, 0), (0, 0)), ((0, 0), (4, 1)), ((0, 0), (8, 2)), ((4, 1), (0, 0)), ((8, 2), (0, 0)), ((4, 1), (4, 1)), ((4, 1), (8, 2)), ((8, 2), (4, 1)), ((8, 2), (8, 2))] [((0, 0), (4, 1)), ((0, 0), (8, 2)), ((4, 1), (8, 2))]
## [(0, 4, 2), (0, 8, 3), (4, 8, 7)]
