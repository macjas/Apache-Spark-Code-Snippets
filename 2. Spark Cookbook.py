from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc)
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.sql.functions import udf
import datetime as datetime
import re as re

# SAMPLE DATA---------------------------------------------------------------
rdd1 = sc.parallelize([('X01',41,'US',3),
                      ('X01',41,'UK',1),
                      ('X01',41,'CA',2),
                      ('X02',72,'US',4),
                      ('X02',72,'UK',6),
                      ('X02',72,'CA',7),
                      ('X02',72,'XX',8)])
# convert to a Spark DataFrame                    
schema1 = StructType([StructField('ID', StringType(), True),
                      StructField('Age', IntegerType(), True),
                      StructField('Country', StringType(), True),
                      StructField('Score', IntegerType(), True)])
df1 = sqlContext.createDataFrame(rdd1, schema1)

rdd2 = sc.parallelize([('ID01',41,'MKTG',3),
                       ('ID02',26,'MKTG',1),
                       ('ID03',11,'MKTG',2),
                       ('ID04',22,'OPS',4),
                       ('ID05',30,'OPS',6),
                       ('ID06',21,'IT',7),
                       ('ID07',25,'IT',8)])
# convert to a Spark DataFrame                    
schema2 = StructType([StructField('ID', StringType(), True),
                     StructField('Age', IntegerType(), True),
                     StructField('Department', StringType(), True),
                     StructField('Tenure', IntegerType(), True)])
df2 = sqlContext.createDataFrame(rdd2, schema2)

rdd3 = sc.parallelize([('X01','2014-02-13T12:36:14.899','2014-02-13T12:31:56.876','sip:7806552624@10.94.2.11'),
                       ('X02','2014-02-13T12:35:37.405','2014-02-13T12:32:13.321',''),
                       ('X03','2014-02-13T12:36:03.825','2014-02-13T12:32:15.229','sip:6477746242@10.94.2.15'),
                       ('XO4','missing','2014-02-13T12:32:36.881','sip:6046179264@10.94.2.11'),
                       ('XO5','2014-02-13T12:36:52.721','2014-02-13T12:33:30.323','sip:4168777435@10.94.2.11')])
schema3 = StructType([StructField('ID', StringType(), True),
                          StructField('EndDateTime', StringType(), True),
                          StructField('StartDateTime', StringType(), True),
                          StructField('ANI', StringType(), True)])
df3 = sqlContext.createDataFrame(rdd3, schema3)

#######################################################################################
# USEFUL CODE SNIPPETS
#######################################################################################
IVRCallLogs.columns         # show all column headers
IVRCallLogs.show(10)        # show first ten rows of a dataframe
IVRCallLogs.take(10)        # show first ten rows of an RDD
sqlContext.clearCache()     # Removes all cached tables from the in-memory cache.

#######################################################################################
# DATA EXPLORATION TASKS
#######################################################################################

# Frequency Counts
df2.Department.distinct().count()


#######################################################################################
# DATA MUNGING TASKS
#######################################################################################

# DEALING WITH DUPLICATES--------------------------------------------------------------

# Select columns by which you want to remove duplicates                
def get_key(x): return "{0}{1}{2}".format(x[0],x[2],x[3])
# create a new RDD to map the columns specified above as keys
m = df1.map(lambda x: (get_key(x),x))
# reduce by key to eliminate duplicates
r = m.reduceByKey(lambda x,y: (x))
# extract only the values of the key-values
r = r.values()

# Alternate approach with dataframe
myDF.groupBy("Name", "Country", "Score").agg("Name", max("Age"), "Country", "Score")

# RESHAPING DATA-------------------------------------------------------------------------

# A functional aproach
def reshape(t):
    out = []
    out.append(t[0])
    out.append(t[1])
    for v in brc.value:
        if t[2] == v:
            out.append(t[3])
        else:
            out.append(0)
    return (out[0],out[1]),(out[2],out[3],out[4],out[5])
    
def cntryFilter(t):
    if t[2] in brc.value:
        return t
    else:
        pass

def addtup(t1,t2):
    j=()
    for k,v in enumerate(t1):
        j=j+(t1[k]+t2[k],)
    return j

def seq(tIntrm,tNext):
    return addtup(tIntrm,tNext)

def comb(tP,tF):
    return addtup(tP,tF)

countries = ['CA', 'UK', 'US', 'XX']
brc = sc.broadcast(countries)
reshaped = rdd1.filter(cntryFilter).map(reshape)
pivot = reshaped.aggregateByKey((0,0,0,0),seq,comb,1)
for i in pivot.collect():
    print i

# The SQL/Hive approach:
df1.registerTempTable("table1")
query = '''
SELECT ID, 
       Age, 
       MAX(CASE WHEN Country='CA' THEN Score ELSE 0 END) AS CA,
       MAX(CASE WHEN Country='UK' THEN Score ELSE 0 END) AS UK,
       MAX(CASE WHEN Country='US' THEN Score ELSE 0 END) AS US,
       MAX(CASE WHEN Country='XX' THEN Score ELSE NULL END) AS XX 
  FROM table1 
GROUP BY ID, Age'''
res = sqlContext.sql(query)
res.show(5)


# Imputing categorical variables------------------------------------------------------------
def impute_region(x):
    if x == 'CA' or x == 'US':
        return 'North America'
    elif x == 'UK':
        return 'Europe'

impute_region = udf(impute_region, StringType())
df4 = df1.withColumn('Region', f(df1.Country))  

# Dealing with Dates-------------------------------------------------------------------------
# Function to calculate time delta
def time_delta(y,x): 
    try:
        end = datetime.strptime(y, '%Y-%m-%dT%H:%M:%S.%f')
        start = datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f')
        delta = (end-start).seconds
        return delta
    except:
        return None
 
time_delta = udf(time_delta, IntegerType())   
df5 = df3.withColumn('Duration', time_delta(df3.EndDateTime, df3.StartDateTime))  

# Apply REGEX to columns--------------------------------------------------------------------
def ani(x):
    try:
        extract = re.search('(\d{10})', x, re.M|re.I)
        out = extract.group(0)
        return out
    except:
        return None
    
ani = udf(ani, StringType())
df6 = df3.withColumn('ANI', time_delta(df3.ANI))  













