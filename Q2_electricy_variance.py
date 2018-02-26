from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        val = line.split(',')
        price = float(val[1])
        yield "", (price,1)

    # def combiner(self, key, values):
    # 	count = 0
    # 	total = 0
    #     sumsq = 0
    # 	for v in values:
    # 		count = count +v[1]
    # 		total = total +v[0]
    # 		sumsq = sumsq + total*total
    # 	yield(key, (total, count, sumsq))

    def reducer(self, key, values):
        count = 0
        total = 0
        sumsq = 0
        for v in values:
            count = count +v[1]
            total = total +v[0]
            sumsq = sumsq + v[0]*v[0]
        var = (sumsq-(total*total)/count)/(count-1)
        yield ("variance", var)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
