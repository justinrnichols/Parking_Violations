import collections
import os
import sys

folders = ['registrationState', 'plateType', 'vehicleBodyType', 'vehicleMake', 'vehicleColor', 'vehicleYear']

files = ['part-r-00000', 'part-r-00001', 'part-r-00002', 'part-r-00003']




minCount = [0, 0, 10000, 1000, 500, 1000]
count = 0

for folder in folders:
    for file in files:
        fr = open('MapReduce/' + folder + '/' + file, 'r')
        lines = fr.readlines()
        sort_by = {'counts': 0, 'amounts': 1, 'averages': 2}
        for task in sort_by:
            line_number = 0
            d = {}
            for line in lines:
                if line_number != 0:
                    key_value = line.split('\t')
                    values = key_value[1].split(',')
                    sort_index = sort_by[task]
                    if int(values[0]) > minCount[count]:
                        d[key_value[0]] = float(values[sort_index])
                line_number += 1
            od = sorted(d.items(), key=lambda x: x[1], reverse=True)
            directory = 'Ordered/' + folder + '/' + file + '/'
            os.makedirs(os.path.dirname(directory), exist_ok=True)
            with open(directory + task + '.txt', 'w+') as fw:
                for key, value in od:
                    print(key, value, file = fw)
    count += 1
