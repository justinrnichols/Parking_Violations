# NYC Parking Violations Big Data Analysis


## Members

1. Justin Nichols: jnich56@lsu.edu
2. Bryce Lee: blee57@lsu.edu


## Big Data Frameworks

1. Apache Hadoop MapReduce
- Eclipse for MapReduce (Java)
2. Apache Spark (specifically PySpark)
- Basic text editor and Linux terminal for PySpark (Python)
3. Apache Hue to visually view and analyze our results.
4. Cloudera virtual machine in pseudo-distributed mode. 


## Project Description

The Parking Violations big data application is aimed at identifying certain correlations between which types of vehicles (based on the plate 
type, vehicle body type, vehicle make, vehicle color, or vehicle year) received the costliest violations depending on the season (spring, 
summer, fall, winter). The project analyzes if, for example, commercial vehicles are more likely to be ticketed more frequently and higher 
during the winter holidays than at other times of year due to the mass amounts of goods and packages being delivered. Another example 
includes, if cars more suitable to drive in warmer months, such as sports cars or convertibles, receive costlier violations in the summer 
compared to those driven in cooler months, such as traditional sedans or SUVs. The application focuses on a 9GB dataset containing parking 
violations issued by the NYC DMV and was implemented in both Java utilizing Hadoop MapReduce and in Python utilizing PySpark. Other than 
analyzing the data, our project was also directed towards comparing and contrasting not only the development process between Hadoop and 
Spark but also the performance. 


## Dataset

The dataset contains parking violation data around the NYC metro area. There are a total of 51 comma separated values for each entry in the 
dataset. We used the information regarding the vehicle body type, vehicle make, vehicle year, vehicle color, registration state, plate type, 
issue date, and violation code. The dataset is around 9GB and contains approximately 42.3 million entries. The dataset can be found at the 
link, https://www.kaggle.com/new-york-city/nyc-parking-tickets.

The dataset contains 4 separate files:
1. Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv (1.87 GB)
2. Parking_Violations_Issued_-_Fiscal_Year_2015.csv (2.86 GB)
3. Parking_Violations_Issued_-_Fiscal_Year_2016.csv (2.15 GB)
4. Parking_Violations_Issued_-_Fiscal_Year_2017.csv (2.09 GB)

However, the dataset size was trimmed for submission purposes and contains the first 10,000 entries for each file, totaling 40,000 entries:
1. Parking_Violations_2013_2014.csv (1.9 MB)
2. Parking_Violations_2015.csv (2.5 MB)
3. Parking_Violations_2016.csv (1.9 MB)
4. Parking_Violations_2017.csv (1.9 MB)

It is important to note that Hadoop MapReduce and PySpark were tested using both the original dataset and the trimmed dataset. Besides the primary dataset, a file named ParkingViolationCodes.txt was used to translate the 98 NYC violation codes used in the dataset into a violation dollar amount. This file was used directly in the programs.

Finally, 2 helper PDF files, regristration_class_body_types_colors.pdf and plate_types_state_codes.pdf, provided by the NYC DMV were used to translate many of the abbreviations and parking technical codes. These files were not directly used in the programs but were used to manually decode the output files. 


## Division of Roles

During the timeline of the project, we have worked simultaneously, equally committing the same amount of work and time. 
Throughout the research and development process, we have successfully worked virtually and have consistently been meeting 
once a week while also keeping constant communication. It has also allowed us to identify aspects of the project that can 
be improved and shift our focus accordingly. We have utilized pair programming to help streamline the development process. 
From finding the data, to writing the code and report, we have both contributed equally and have kept an open collaborative 
environment to allow ideas to bounce off one another. 


## Components

### Hadoop MapReduce
1. Hadoop Driver
2. Hadoop Partitioner
3. Hadoop Mapper
4. Hadoop Reducer

### Spark
1. PySpark

View the project final report to get detailed component descriptions.


## Results, Evaluations, Conclusions

View the project final report to get a detailed analysis on our findings.


## How to Run

### General Instructions

1. Open up the Cloudera-Training-CAPSpark-Student-VM-cdh5.4.3a-vmware virtual machine in VMWare. 
Download VMWare using this [Link](https://www.vmware.com/products/workstation-player/workstation-player-evaluation.html) and the VM using this 
[Link](https://lsumail2-my.sharepoint.com/:u:/g/personal/klee76_lsu_edu/EaKfXeEzunNAt-OXW7Lhy4MByuJ_gP1Hxxw41N5pXZ15bg?e=R1xfI7). 
2. Copy and paste the ParkingViolations.zip deliverables file in the `/home/training` directory on the VM.
3. Extract the folder from the zip file.
4. Open a terminal window and use the command:<br>
	`hdfs dfs -put ParkingViolations/Data`
	<br>- This will put the csv files containing the data in the Hadoop Distributed File System.
	<br>- Note, the csv files included in the deliverables are smaller versions of the original files, since the VM has, by default, limited disk space.

### Hadoop MapReduce

1. To execute the MapReduce task, use the command:<br>
	`hadoop jar ParkingViolations/Code/ParkingViolations.jar parking_violations.ParkingViolationsDriver -D taskType=<taskType> Data output`
	<br>- where `<taskType>` is one of the options: `“registrationState”`, `“plateType”`, `“vehicleBodyType”`, `“vehicleMake”`, `“vehicleColor”`, `“vehicleYear”`
2. When the MapReduce task completes, open up Firefox on the VM and click the Hue bookbar at the top.
3. Click the File System tab, then view the `output` directory to see the 4 output files, part-r-00000, part-r-00001, part-r-00002, part-r-00003, and part-r-00004.

### PySpark

1. To execute the PySpark task, use the command:<br>
	`spark-submit ParkingViolations/Code/parking_violations.py Data <taskType>`
	<br>- where `<taskType>` is one of the options: `registrationState`, `plateType`, `vehicleBodyType`, `vehicleMake`, `vehicleColor`, `vehicleYear`
2. When the PySpark task completes, open up Firefox on the VM and click the Hue bookbar at the top.
3. Click the File System tab, then view the `output2` directory to see the 4 output files, part-r-00000, part-r-00001, part-r-00002, part-r-00003, and part-r-00004.


### Appendix
1. README.md
2. Code/
- ParkingViolationsDriver.java
- ParkingViolationsPartitioner.java
- ParkingViolationsMapper.java
- ParkingViolationsReducer.java
- parking_violations.py
- ParkingViolations.jar
3. Data/
- ParkingViolationCodes.txt
- Parking_Violations_2013_2014.csv
- Parking_Violations_2015.csv
- Parking_Violations_2016.csv
- Parking_Violations_2017.csv
4. Helpers/
- plate_types_state_codes.pdf
- regristration_class_body_types_colors.pdf
5. Output/
- Contains outputs for both MapReduce and PySpark for the task types `registrationState`, `plateType`, `vehicleBodyType`, `vehicleMake`, `vehicleColor`, `vehicleYear`.
- MapReduce also contains an alternate version where the data is sorted by count, total dollars, and average to easily visualize and evaluate.
- analyze.py
6. Reports/
- Report1
- Report2
- Final_Report
- Slides