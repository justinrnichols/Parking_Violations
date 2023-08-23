from pyspark import SparkContext
import operator
import sys
import time

# Get the start time
start = time.time()

sc = SparkContext()


''' 
variable declarations:
columns: stores column name and column index to use for reference
options: stores the different options available to pass when executing a pyspark job
seasons: stores the season name and season index to use for reference
month_season: stores the month and its season to use for reference
'''
columns = {'Vehicle Body Type': 6, 'Vehicle Make': 7, 'Vehicle Year': 35, 'Vehicle Color': 33, 
                'Registration State': 2, 'Plate Type': 3, 'Issue Date': 4, 'Violation Code': 5}
options = {'vehicleBodyType': 6, 'vehicleMake': 7, 'vehicleYear': 35, 'vehicleColor': 33, 'registrationState': 2, 'plateType': 3}
seasons = {'Summer': 0, 'Fall': 1, 'Winter': 2, 'Spring': 3}
month_season = {0: 'Winter', 1: 'Winter', 2: 'Spring', 3: 'Spring', 4: 'Spring', 5: 'Summer', 6: 'Summer', 7: 'Summer', 8: 'Fall', 9: 'Fall', 10: 'Fall', 11: 'Winter'}


# gather command line args, which includes the directory information and taskType
file = sys.argv[1]
parking_violations = sc.textFile(file)
if len(sys.argv) < 3: # default to vehicleMake if no taskType is given
    task_type = 'vehicleMake'
else:
    task_type = str(sys.argv[2])
if task_type not in options.keys(): # check if the taskType value is valid
    task_type = 'vehicleMake'
    raise ValueError('Invalid option')


# gather violation codes and dollar amounts from the external file
# violation_info stores (violation code, violation amount)
# the first line (legend) is filtered out
violations = sc.textFile('Data/parking_violation_codes.txt').map(lambda line: line.split('\t')).filter(lambda line: 'VIOLATION CODE' not in line).map(lambda line: (line[0], line[2]))
violation_info = {}
violation_codes = list(violations.keys().collect())
violation_amounts = list(violations.values().collect())
for i in range(len(violation_codes)):
    violation_info[int(violation_codes[int(i)])] = int(violation_amounts[int(i)])


# split the data by commas
split_data = parking_violations.map(lambda line: line.split(','))

# filter out the legend on each input file (first line)
filter_legend = split_data.filter(lambda line: 'Summons Number' not in line)

# filter out columns missing data (removes lines with columns < 35 or missing task type, issue date, or violation code)
filter_missing_data = filter_legend.filter(lambda line: len(line) >= 35).filter(lambda line: line[options[task_type]] and line[columns['Issue Date']] and line[columns['Violation Code']])

# filter out columns with misformatted data (removes lines with misformatted issue date or invalid violation code)
filter_misformatted_data = filter_missing_data.filter(lambda line: str(line[columns['Issue Date']][0:2]).isdigit()).filter(lambda line: str(line[columns['Violation Code']]).isdigit()).filter(lambda line: int(line[columns['Violation Code']]) in violation_info)


# maps each value to pair ([season, task_type], violation_code) 
mapper = filter_misformatted_data.map(lambda line: ((seasons[month_season[int(line[columns['Issue Date']][0:2])-1]], line[options[task_type]]), line[columns['Violation Code']]))

# collects the number of values per key 
countsByKey = sc.broadcast(mapper.countByKey()).value

# filter out keys that have less than 5 values
filter_reducer = mapper.filter(lambda kv: countsByKey[kv[0]] >= 5)

# adjust pair to be ([season, task_type], violation_amount)
adjust_key_value = filter_reducer.map(lambda kv: (kv[0], violation_info[int(kv[1])]))

# reduce the values by key and add up the total dollar amount and divide by the number of values associated to the key to find the average
reducer_avg = adjust_key_value.reduceByKey(operator.add).map(lambda kv: (kv[0], str(countsByKey[kv[0]]) + ',' + str(kv[1]) + ',' + str(round(kv[1]/countsByKey[kv[0]], 2))))

# adjust pair to be (season, [task_type, violation_amount])
adjust_key_value = reducer_avg.map(lambda kv: (kv[0][0], str(kv[0][1]) + ',' + kv[1]))

# sort the results by key and partition the data by season, having 1 output file per season
# outputs each pair in the form (task_type, [count, total_dollars, average])
sorter_partitioner = adjust_key_value.sortBy(lambda kv: kv[1][0]).partitionBy(4, lambda x: int(x)).map(lambda kv: (kv[1].split(',')[0], kv[1].split(',')[1:]))

# output sorted partitions to file
sorter_partitioner.saveAsTextFile('output2')

# Get the end time
end = time.time()
print('Total execution time: ', end - start)