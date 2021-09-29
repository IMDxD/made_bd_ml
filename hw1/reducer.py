import sys

total_variance = 0
total_mean = 0
total_count = 0


for line in sys.stdin:
    mean, variance, count = map(float, line.rstrip().split())
    total_variance = (
            (total_variance * total_count + variance * count) /
            (total_count + count) +
            total_count * count *
            (
                    (total_mean - mean) /
                    (total_count + count)
            ) ** 2
    )
    total_mean = (total_mean * total_count + mean * count) / (total_count + count)
    total_count += count

print("{0}\t{1}".format(total_mean, total_variance))
