#Crime Investigation using Call Data Records
#Handling Big Data

from operator import add
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pandas import DataFrame,Series
import numpy as np
data=sc.textFile('/home/achilles/Documents/csv/calls.csv',use_unicode=False)
cdrm=data.map(lambda x: x.split(','))
cdr=cdrm.collect()
#Input number
num=input("Enter phone number: ")
cnt=cdrm.filter(lambda line: num in line[0]).count()
col="user other direction duration time"
fields=[StructField(field_name, StringType(), True) for field_name in col.split()]
colnames=StructType(fields)
df=sqlContext.createDataFrame(cdr,colnames)
dataframe=df.registerTempTable("calls")
outrdd=sqlContext.sql("SELECT other,duration FROM calls WHERE user="+num+" AND time LIKE '%Thu Sep 23 %'")
sort=outrdd.rdd
sort1=sort.map(lambda y:(y[0],y[1]))
#sort1=sort1.reduceByKey(add)
sort2=sort.map(lambda w: (w[0],1))
sort2=sort2.reduceByKey(add)
#sort=sort.sortByKey(False)
suspects1=sort1.take(20)
suspects2=sort2.take(20)
#for suspects 
cdrd=sc.parallelize(suspects1)
wikis = cdrd.map(lambda p: (p[0], p[1]))
fields = [StructField("other", StringType(), True),
StructField("duration", FloatType(), True)]
schema = StructType(fields)
wikis = cdrd.map(lambda p: (p[0],float(p[1])))
#wikis=cdrd.map(lambda y:(y[0],y[1]))
wikis.reduceByKey(add)
df = sqlContext.createDataFrame(wikis, schema)
#dataframe=df.registerTempTable("sorted")
#output=sqlContext.sql("SELECT * FROM sorted")
#output.show()
#for suspects2
cdrd2=sc.parallelize(suspects2)
wikis2 = cdrd2.map(lambda p: (p[0], p[1]))
fields2 = [StructField("other", StringType(), True),
StructField("no_of_calls", FloatType(), True)]
schema2 = StructType(fields2)
wikis2 = cdrd2.map(lambda p: (p[0],float(p[1])))
df2 = sqlContext.createDataFrame(wikis2, schema2)
#dataframe2=df2.registerTempTable("sorted2")
dfnew=df.join(df2,on='other')
#dfnew.toPandas()
dataframe=dfnew.registerTempTable("training")
output=sqlContext.sql("SELECT other,SUM(duration) AS duration,COUNT(no_of_calls) AS no_of_calls FROM training GROUP BY other")
tocsv=output.toPandas()
#testing data
tocsv

#Machine Learning Code

import pandas as pd
import numpy as np
from pandas import DataFrame,Series
import sklearn
from sklearn import tree
from sklearn.metrics import *
from sklearn.neighbors import KNeighborsClassifier
#training data
data=pd.read_csv('file.csv')
df=DataFrame(data,index=None)
lst=[]
#features=[[df['duration'],df['no_of_calls']],]
for r in np.arange(44):
    l=[data['duration'][r],data['no_of_calls'][r]]
    lst.append(l)
features=lst
#print lst
label=df['status']
label.tolist()
#print label
clf=KNeighborsClassifier()
clf.fit(features, label)
sus_no=input('Enter the suspects number:')
dur=tocsv['duration'][sus_no]
noc=tocsv['no_of_calls'][sus_no]
stat=clf.predict([[dur,noc]])
if stat==0:
    print('Susepect')
if stat==1:
    print('May be Suspect')
if stat==2:
    print('Innocent')
#Checking the accuracy
stat1=clf.predict(features)
print('Accuracy:')
print accuracy_score(label,stat1)