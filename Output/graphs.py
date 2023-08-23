import matplotlib.pyplot as plt
import os

folders = ['registrationState', 'plateType', 'vehicleBodyType', 'vehicleMake', 'vehicleColor', 'vehicleYear']

files = ['part-r-00000', 'part-r-00001', 'part-r-00002', 'part-r-00003']

minCount = [0, 0, 10000, 1000, 500, 1000]
count = 0

for folder in folders:
	kv1, kv2, kv3 = {}, {}, {}
	for file in files:
		fr = open('MapReduce/' + folder + '/' + file, 'r')
		lines = fr.readlines()
		line_number = 0
		for line in lines:
			if line_number != 0:
				key_value = line.split('\t')
				key = key_value[0]
				values = key_value[1].split(',')
				count_value = float(values[0])
				total_dollars_value = float(values[1])
				average_value = float(values[2])
				if count_value > minCount[count]:
					if key not in kv1:
						kv1[key] = count_value
						kv2[key] = total_dollars_value
						kv3[key] = average_value
					else:
						kv1[key] += count_value
						kv2[key] += total_dollars_value
						kv3[key] += average_value
			line_number += 1
	count += 1
	count_keys, total_dollars_keys, average_keys, count_values, total_dollars_values, average_value = [], [], [], [], [], []
	for i in range(10):
		keyval1 = max(kv1.items(), key = lambda k : k[1])
		count_keys.append(keyval1[0])
		count_values.append(keyval1[1])
		kv1.pop(keyval1[0])

		keyval2 = max(kv2.items(), key = lambda k : k[1])
		total_dollars_keys.append(keyval2[0])
		total_dollars_values.append(keyval2[1])
		kv2.pop(keyval2[0])

		keyval3 = max(kv3.items(), key = lambda k : k[1])
		average_keys.append(keyval3[0])
		average_value.append(keyval3[1]/4)
		kv3.pop(keyval3[0])
	labels = ['counts', 'amounts', 'averages']
	all_keys = [count_keys, total_dollars_keys, average_keys]
	all_values = [count_values, total_dollars_values, average_value]
	print(all_keys)
	print(all_values)
	print()
	for i in range(len(labels)):
		plt.figure(figsize=(9,8))
		plt.bar(all_keys[i], all_values[i], color='royalblue', width = 0.5)
		plt.title(folder + ': ' + labels[i])
		plt.xlabel('key')
		plt.ylabel(labels[i])
		plt.ticklabel_format(style='plain', axis='y')
		# plt.show()
		directory = 'Graphs/' + folder + '/'
		os.makedirs(os.path.dirname(directory), exist_ok=True)
		plt.savefig(directory + labels[i] + '.png')

