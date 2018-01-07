
# coding: utf-8

# In[1]:


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


# In[2]:


# Candidati Elezioni
users = [
    {'user':config['USER1'],'user_id':config['USER1_ID_TW']},
    {'user':config['USER2'],'user_id':config['USER2_ID_TW']},
    {'user':config['USER3'],'user_id':config['USER3_ID_TW']},
    {'user':config['USER4'],'user_id':config['USER4_ID_TW']},
    {'user':config['USER5'],'user_id':config['USER5_ID_TW']},
    {'user':config['USER6'],'user_id':config['USER6_ID_TW']},
    {'user':config['USER7'],'user_id':config['USER7_ID_TW']}
]


# In[3]:


# Authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)


# In[4]:


# get today's date
todays_date = datetime.datetime.now()


# In[30]:


# get lasts tweet
def get_last_tweet(tw_user, tw_id, api, date):
    timeline = api.user_timeline(tw_id)
    dt = timeline[0].created_at
    id_post = timeline[0].id
    msg = timeline[0].text
    likes = timeline[0].favorite_count
    
    last_tweet = {'user':tw_user,'dt_rif':date,'dt_post':dt,'id_post':id_post, 'msg':msg, 'likes':likes}
    
    return last_tweet


# In[31]:


tw_posts = []
for user in users:
    tw_posts.append(get_last_tweet(user['user'],user['user_id'],api,todays_date))


# In[32]:


df = pd.DataFrame(tw_posts)


# In[33]:


df


# In[34]:


# get database connection
db=config['DATABASE_ELE']
schema=config['SCHEMA_ELE']
engine = create_engine(db)


# In[35]:


# write to db
df.to_sql('tw_posts', engine, schema=schema, if_exists='append')

