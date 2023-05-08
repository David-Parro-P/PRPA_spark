# PRPA_spark

### Quijote
word_count.py archivo.txt  --> devuelve el numero de palabras de un archivo dado

activiad_dq.py archivo.txt destino ratio, devuelve un archivo donde quita las palabras si random en [0,1] no supera ratio.

### Tricilos
Una vez limpiado de repeticiones y aristas (X,X)

Para cada (X, Y) creamos la lista [((X, Y), (Y, ?) | (Y, ?) € {Aristas Grafo} ] con un flatmap queda todo en una lista.

Después basta comprobar si (X,?) está en el grafo para ver si tenemos un triciclo.
