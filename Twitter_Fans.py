
# coding: utf-8

# In[32]:


# Import
import os
import tweepy
import sys
import datetime
import pandas as pd
import numpy as np
from tweepy.streaming import StreamListener
from tweepy import Stream
from sqlalchemy import create_engine

config = {}
config_path = os.path.join(os.path.abspath('../../'))
config_name = 'config.py'
config_file = os.path.join(config_path,config_name)
exec(open(config_file).read(),config)


# Key and Secret
consumer_key=config['TWITTER_KEY']
consumer_secret=config['TWITTER_SECRET']
access_token=config['TOKEN']
access_token_secret=config['TOKEN_SECRET']


# In[14]:


# Candidati Elezioni
users = [
    {'user':config['USER1'],'user_id':config['USER1_ID_TW']},
    {'user':config['USER2'],'user_id':config['USER2_ID_TW']},
    {'user':config['USER3'],'user_id':config['USER3_ID_TW']},
    {'user':config['USER4'],'user_id':config['USER4_ID_TW']},
    {'user':config['USER5'],'user_id':config['USER5_ID_TW']},
    {'user':config['USER6'],'user_id':config['USER6_ID_TW']}
]


# In[15]:


# Authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)


# In[26]:


# get today's date
todays_date = datetime.datetime.now()


# In[27]:


# get fans
def get_tw_fans(tw_user, tw_id, api, date):
    user = api.get_user(tw_id)
    fan = user.followers_count
    fan_d = {'dt_rif':date,'user':tw_user,'tw_fans':fan}
    return fan_d


# In[28]:


tw_fans = []
for user in users:
    tw_fans.append(get_tw_fans(user['user'],user['user_id'],api,todays_date))


# In[29]:


df = pd.DataFrame(tw_fans)


# In[30]:


df


# In[33]:


# get database connection
db=config['DATABASE_ELE']
schema=config['SCHEMA_ELE']
engine = create_engine(db)


# In[35]:


# write on db
df.to_sql('tw_fans', engine, schema=schema, if_exists='append')

