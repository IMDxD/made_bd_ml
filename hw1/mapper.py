import csv
import sys

total_count = 0
total_sum = 0
total_square_sum = 0
for row in csv.reader(sys.stdin, delimiter=","):
    if len(row) > 8:
        price = row[9]
        if price != "price":
            price = float(price)
            total_sum += price
            total_square_sum += price ** 2
            total_count += 1
total_mean = total_sum / total_count
total_variance = total_square_sum / total_count - total_mean ** 2
print('{0}\t{1}\t{2}'.format(total_mean, total_variance, total_count))
