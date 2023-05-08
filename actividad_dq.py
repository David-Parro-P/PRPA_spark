#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 21 12:07:15 2023

@author: davidp
"""
from pyspark import SparkContext
import random
import sys
def main(infilename, outfilename, ratio):
        with SparkContext() as sc: 
            sc.setLogLevel("ERROR")
            data        = sc.textFile('quijote.txt')
            new_content = data.filter(lambda x: random.random() > ratio)
            # Es pequeño, se puede hacer
            new_content.coalesce(1,True).saveAsTextFile(path="test")


if __name__=="__main__":
    if len(sys.argv)<4:
        print(f"Usage: {sys.argv[0]} <infilename> <outfilename> <ratio")
        exit(1)
    infilename = sys.argv[1]
    outfilename = sys.argv[2]
    ratio = float(sys.argv[3])
    main(infilename, outfilename, ratio)
