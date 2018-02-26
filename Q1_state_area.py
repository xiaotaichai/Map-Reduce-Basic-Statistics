from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        val = line.split(',')
        area = float(val[3])
        yield "", (area,1)

    def combiner(self, key, values):
    	count = 0
    	total = 0
    	for v in values:
    		count = count +v[1]
    		total = total +v[0]
    		area = v[0]
    	yield(key, (total, count, area))

    def reducer(self, key, values):
    	all_area = []
    	for v in values:
    		count = v[1]
    		total = v[0]
    		ave = total/count
    		area = v[2]
    		all_area.append(area)
    	yield (key, ave)
    	yield (key, max(all_area))
    	yield (key, min(all_area))


if __name__ == '__main__':
    MRWordFrequencyCount.run()
