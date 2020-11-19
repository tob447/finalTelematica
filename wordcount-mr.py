from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
#       for w in line.decode('utf-8', 'ignore').split():
        words=line.split()
	word=words[0].split(",")
	if not "price" in word:
		yield (word[0],(float(word[1]),word[2]))
		#yield word[0]+,word[1]

    def reducer(self, key, values):
	maxValue=0
	minValue=1000000000000000000
	maxDate=0
	minDate=0

	for value in values:
		if value[0]>maxValue:
			maxValue=value[0]
			maxDate=value[1]
		if value[0]<minValue:
			minValue=value[0]
			minDate=value[1]	

        yield (key,(maxDate,minDate))



class MRWordSiempreCrece(MRJob):
    def mapper(self, _, line):
#       for w in line.decode('utf-8', 'ignore').split():
        words=line.split()
        word=words[0].split(",")
        if not "price" in word:
                yield (word[0],(float(word[1]),word[2]))
                #yield word[0]+,word[1]

    def reducer(self, key, values):
        maxValue=0
       	siempreCrece=True

        for value in values:
                if value[0]>maxValue:
                        maxValue=value[0]
                if value[0]<maxValue:
			siempreCrece=False
                

        yield (key,("Siempre Crece o se mantiene",siempreCrece))




if __name__ == '__main__':
    MRWordFrequencyCount.run()
    MRWordSiempreCrece.run()







