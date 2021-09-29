import csv
import sys

for row in csv.reader(sys.stdin, delimiter=","):
    if len(row) > 8:
        price = row[9]
        if price != "price":
            print('{0}\t0\t1'.format(price))
