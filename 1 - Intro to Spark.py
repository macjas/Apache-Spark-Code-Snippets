''' Notes:
1. To be run from the Pyspark Shell
2. Using .collect() allows you convert RDDs into common Python objects including Pandas! The cost of doing so is that all data that parses through collect() is stored into memory. 
2. # To use IPython as the Spark shell, CD into Spark Home  and run the command:
   IPYTHON=1 IPYTHON_OPTS="--pylab" /usr/lib/spark/bin/pyspark
3. SAMPLE WORKFLOW: HDFS > RDD > Python/Pandas > RDD > Push to Hive
   
'''
import numpy as np
import matplotlib.pyplot as plt
from collections import OrderedDict

##############################################################################################################
# 1. DATA INPUT
##############################################################################################################
# If the data is not in HDFS, use 'file:///' to specify the exact directory location
user_data = sc.textFile("file:///home/cloudera/datasets/ml-100k/u.user") 
movie_data = sc.textFile("file:///home/cloudera/datasets/ml-100k/u.item")
rating_data = sc.textFile("file:///home/cloudera/datasets/ml-100k/u.data") 

# the .textfile() function creates an RDD
type(user_data) 

# splits on the seperator to create a pipelineRDD. Doing this allows us to index fields/columns
user_fields = user_data.map(lambda line: line.split("|")) 
movie_fields = movie_data.map(lambda line: line.split("|")) 
rating_fields = rating_data.map(lambda line: line.split("\t")) 

# show the columns in the RDDs
print user_fields.first()  
print movie_fields.take(20)  
print rating_fields.take(20) 


##############################################################################################################
# 2. EXPLORATORY DATA ANALYSIS
##############################################################################################################
# In the script below, we do most EDA in Pyspark. Alternately, you could convert the RDD to a dataframe and do EDA
# using Pandas.

# Count rows:
num_ratings = rating_data.count()

# Get Categorical Levels----------------------------------------------------
# Find levels of a categorical variable convert to a Python list and pass on a math function like .count()
num_users = user_fields.map(lambda fields: fields[0]).distinct().count() 
num_genders = user_fields.map(lambda fields: fields[2]).distinct().count()
num_occupations = user_fields.map(lambda fields: fields[3]).distinct().count()
num_zipcodes = user_fields.map(lambda fields: fields[4]).distinct().count()
num_movies = movie_fields.map(lambda fields: fields[1]).distinct().count()
print 'Users: {}'.format(num_users), '\nGenders: {}'.format(num_genders), '\nOccupations: {}'.format(num_occupations), '\nZIP codes: {}'.format(num_zipcodes), '\nMovies: {}'.format(num_movies)

# Frequency Counts----------------------------------------------------------
# This is similar to PROC FREQ but outputs a Python dictionary instead
count_by_rating = rating_fields.map(lambda fields: fields[2]).countByValue()
count_by_rating = OrderedDict(sorted(count_by_rating.items()))   # sort the dictionary from 1- 5
print '\nCounts by Rating: {}'.format(count_by_rating)

# Summary Statistics--------------------------------------------------------
ratings = rating_fields.map(lambda fields: int(fields[2])) 
print ratings.stats()

# Counts by Variables:------------------------------------------------------ 
# To count ratings by user, we will first create a pipelinedRDD of the user ID as key and rating as value and then we will groupby key.
user_ratings_grouped = rating_fields.map(lambda fields:(int(fields[0]),int(fields[2]))).groupByKey() # creates pipelined RDD
# Count ratings per user
user_ratings_byuser = user_ratings_grouped.map(lambda (k, v): (k, len(v)))
# Extract the ratings from the RDD as a Python list and plot
ratings = (user_ratings_byuser.map(lambda (k, v):int(v))).collect()
ratings = np.array(ratings)
plt.hist(ratings, color="#6495ED", alpha=.5)

##############################################################################################################
# 3. DATA VISUALIZATION
##############################################################################################################
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Visualize the distribution of ages 
ages = user_fields.map(lambda x: int(x[1])).collect()
ages = np.array(ages)
plt.hist(ages, normed=True, color="#6495ED", alpha=.5)
sns.kdeplot(ages)

# Visualize counts of rating
keys = np.array(count_by_rating.keys())
values = np.array(count_by_rating.values())
sns.barplot(x = keys, y = values)

##############################################################################################################
# 4. DATA PROCESSING
##############################################################################################################

# Bad data:--------------------------------------------------------- 
# A function to only get 'four digit' years. E.g. 19955, strings or NAs will be replaced with 0
def convert_year(x):
  try:
    return int(x[-4:])
  except:
    return 0 # will return 0 if year variable >4 digits


# Replace all erroneous years values with 0 using the convert_year function
years = movie_fields.map(lambda fields: fields[2]).map(lambda x: convert_year(x)).collect() 
years = np.array(years)   

# find mean of years without the bad data points
mean_year = np.mean(years[years != 0])
# find median of years without the bad data points (incase you want to use median to impute)
median_year = np.median(years[years != 0])
# show index of where bad values are found
index_bad_data = np.where(years == 0)[0][0] 
# replace bad points with the mean of year
years[index_bad_data] = mean_year

print "Mean year of release: %d" % mean_year
print "Median year of release: %d" % median_year
print "Index of bad datapoints remaining:" % np.where(years == 1900)[0]

# Encoding categorical data:----------------------------------- 
# obtain a list of all factors in a categorical variable
all_occupations = user_fields.map(lambda fields: fields[3]).distinct().collect()
all_occupations.sort()

# Assign indexes to each factor of the categorical variable
idx = 0
all_occupations_dict = {}
for o in all_occupations:
    all_occupations_dict[o] = idx
    idx +=1
 
# sort and print the dictionary
all_occupations_dict = OrderedDict(sorted(all_occupations_dict.items()))   
print all_occupations_dict
    
# Dummy Variables:-------------------------------------------- 
# to obtain dummy variables for occupation = 'programmer' in the form of an array
K = len(all_occupations_dict)
binary_x = np.zeros(K)
k_programmer = all_occupations_dict['programmer']
binary_x[k_programmer] = 1
print "Binary feature vector: %s" % binary_x, 
print "Length of binary vector: %d" % K


##############################################################################################################
# 5. DATA IMPUTATION
##############################################################################################################
# Extracting DateTime:----------------------------------------
def ParseDatetime(ts):
    import datetime
# you will need to change this depending on the format of date/time in the raw data
    return datetime.datetime.fromtimestamp(int(ts)) 
    
timestamps = rating_fields.map(lambda fields: fields[3])
hour = timestamps.map(lambda ts: ParseDatetime(ts).hour)  # extract just the hour portion into an RDDpipeline
hour.take(5)

# This function iterates through the dictionary of defined terms
def assign_tod(hr):
  times_of_day = {
    'morning' : range(7, 12),
    'lunch' : range(12, 14),
    'afternoon' : range(14, 18),
    'evening' : range(18, 23),
    'night' : range(23, 7)
  }
  for k, v in times_of_day.iteritems():
    if hr in v:
      return k
      
time_of_day = hour.map(lambda hr: assign_tod(hr))
time_of_day.take(5)


from pyspark.sql import SQLContext
from pyspark.sql.types import *
sqlContext = SQLContext(sc)


IVR = sqlContext











