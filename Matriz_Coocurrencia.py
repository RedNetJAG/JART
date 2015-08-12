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

labelslist = onlyLabels.collect()
labels = np.array(labelslist)
Matriz = np.c_[labels, Coomatriz]
