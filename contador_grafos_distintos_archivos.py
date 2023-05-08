#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  8 14:01:50 2023

@author: davidp
"""

from pyspark import SparkContext
import sys

def mapper(line):
    edge = line.split(',')
    n1 = edge[0]
    n2 = edge[1]
    
    if n1 < n2:
        return [(n1, n2)]
    if n1 >= n2:
        return [(n2,n1)]



SAMPLE = 15
# Asumimos que el input es la lista de archivos entre los que esta distribuido
# El grafo
sc = SparkContext()

# Combinamos todos los archivos en un rdd
arista =  sc.textFile(sys.argv[1][0])
for file in sys.argv[1]:
    aux    = sc.textFile(file)
    arista = arista.union(aux)
    
# Ya podemos aplicar el codigo del primer ejercicio
print('textFile', arista.collect())

arista = arista.flatMap(mapper)

arista = arista.distinct()
arista = arista.filter(lambda x: x[0] != x[1])
print('flatMap', arista.collect())


vertices = arista.flatMap(lambda e: e).distinct()

print("Vertices: ", vertices.collect())
# Cogemos todas las aristas
aux_store = vertices.collect()
print("Aux_store", aux_store)
# Creamos los posibles triangulos
triangle_arista = arista.flatMap(lambda e: [(e[0], e[1], x) for x in aux_store if x not in e])

print("T_arista: ", triangle_arista.collect())
edge_pairs = arista.flatMap(lambda e: [((e[0], e[1]), e[1]), ((e[1], e[0]), e[0])])
print("Edge_Pairs: ", edge_pairs.collect())
# Preparamos el formato para el join
triangle_pairs = triangle_arista.flatMap(lambda e: [((e[0], e[1]), e[2])])
print("NEW_T", triangle_pairs.collect())

joined_pairs = triangle_pairs.join(edge_pairs)
print("JOIN: ", joined_pairs.collect())

aux_arista = arista.collect()


# Buscamos los triangulos compatibles
triangle_count_pairs = joined_pairs.filter(lambda x: (x[0][0], x[1][0]) in aux_arista)
triangle_count_pairs = triangle_count_pairs.distinct()
triangle_count_pairs = triangle_count_pairs.filter(lambda x: (x[1][0],x[1][1]) in aux_arista)
print("posibles:", triangle_count_pairs.collect())
resultado = triangle_count_pairs.count()
print("Resultado: ", resultado)



sc.stop()
