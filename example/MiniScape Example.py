
# coding: utf-8

# In[4]:

get_ipython().magic(u'load_ext autoreload')
get_ipython().magic(u'autoreload 2')

import sys
import csv
import pandas as pd

import scape 
import scape as st
import scape.registry as sr
import scape.pandas as spd
import scape.spark as ss 


# In[1]:

simpsons = [{'Name':'Homer', 'Age':38.0}, 
            {'Name':'Marge','Age':38.0}, 
            {'Name':'Bart','Age':10.0}, 
            {'Name':'Lisa','Age':7.0},
            {'Name':'Maggie','Age':1.0}]
simpsons_meta = {
     "Name" : { "tags" : ["first_name"], "dim":"name"},
     "Age" : { "tags" : ["age"], "dim":"years"},
 }


# In[3]:

pd_datasource = spd.datasource(lambda: pd.DataFrame.from_dict(simpsons), simpsons_meta)
pd_df = pd_datasource.connect()
pd_df


# In[4]:

pd_df.or_filter("age:", 38).or_filter('name','Homer')


# In[5]:

def load_dataframe():
    spark_df = sqlContext.createDataFrame(simpsons,None,1)    
    return spark_df
spark_datasource = ss.datasource(load_dataframe, simpsons_meta)
spark_df = spark_datasource.connect()


# In[7]:

spark_df.or_filter("age:", 38).or_filter('name','Homer').collect()

