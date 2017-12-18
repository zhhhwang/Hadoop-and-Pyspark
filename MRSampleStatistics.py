from mrjob.job import MRJob
from mrjob.step import MRStep


class MRSampleStatistics(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_sum,
                   combiner=self.combiner_1,
                   reducer=self.reducer_1),
            MRStep(reducer=self.mapper_calculate)
        ]

    def mapper_sum(self, _, string):
        label, value = string.split()
        values = [float(value), float(value)**2, 1]
        yield label, values

    def combiner_1(self, index, values):
        summation = [sum(i) for i in zip(*[x for x in values])]
        yield(index, summation)

    def reducer_1(self, index, sample):
        summation = [sum(i) for i in zip(*[x for x in sample])]
        yield (index, summation)

    def mapper_calculate(self, index, summations):
        summation = [x for x in summations][0]
        counting = summation[2]
        mean = summation[0]/summation[2]
        variance = summation[1] / summation[2] - (summation[0]/summation[2])**2
        yield(index, [counting, mean, variance])

if __name__ == '__main__':
    MRSampleStatistics.run()