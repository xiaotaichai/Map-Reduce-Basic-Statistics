from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        val = line.split(',')
        population = float(val[4])
        yield "", (population,1)

    def combiner(self, key, values):
    	count = 0
    	total = 0
    	for v in values:
    		count = count +v[1]
    		total = total +v[0]
    		population = v[0]
    	yield(key, (total, count, population))

    def reducer(self, key, values):
    	all_population = []
    	for v in values:
    		count = v[1]
    		total = v[0]
    		ave = total/count
    		population = v[2]
    		all_population.append(population)
    	yield (key, ave)
    	yield (key, max(all_population))
    	yield (key, min(all_population))


if __name__ == '__main__':
    MRWordFrequencyCount.run()





## Calculate the largest, smallest, and average (mean) population for a state. Calculate the largest, smallest, and average (mean) area for a state
# from mrjob.job import MRJob
# import re

# WORD_RE = re.compile(r"[\w']+")

# class MRWordFrequencyCount(MRJob):

#     def mapper(self, _, line):
#         val = line.split(',')
#         name = val[0]
#         population = float(val[4])
#         yield name, (population,1)

#     def combiner(self, key, values):
#     	count = 0
#     	total = 0
#     	for v in values:
#     		count = count +v[1]
#     		total = total +v[0]
#     		population = v[0]
#     	yield(key, (total, count, population))

#     def reducer(self, key, values):
#     	all_population = []
#     	for v in values:
#     		count = v[1]
#     		total = v[0]
#     		ave = total/count
#     		population = v[2]
#     		all_population.append(population)
#     	yield (key, ave)
#     	yield (key, max(all_population))
#     	yield (key, min(all_population))


# if __name__ == '__main__':
#     MRWordFrequencyCount.run()