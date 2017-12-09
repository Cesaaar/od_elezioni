
# coding: utf-8

# In[2]:


# Import
import os
import tweepy
import sys
import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

config = {}
config_path = os.path.join(os.path.abspath('../../'))
config_name = 'config.py'
config_file = os.path.join(config_path,config_name)
exec(open(config_file).read(),config)


# In[3]:


# get database connection
db=config['DATABASE_ELE']
schema=config['SCHEMA_ELE']
engine = create_engine(db)

user1 = config['USER1']
user2 = config['USER2']
user3 = config['USER3']


# In[4]:


# get today's date
todays_date = datetime.datetime.now()


# In[5]:


# LAST Twitter Post
cur = engine.execute(
    '''     
    SELECT 
        dt_post as dt
        ,msg as msg
        ,"user" as user
        ,likes as likes
        ,'twitter' as sorgente
        ,''' "'" + str(todays_date) + "'" ''' as dt_rif
    FROM ''' + schema + '''."tw_posts"
    WHERE 
        "user" in (''' "'" + user1 + "'" ''',''' "'" + user2 + "'" ''',''' "'" + user3 + "'" ''')
        and dt_rif=(select max(dt_rif) from ''' + schema + '''."tw_posts")
            ''')
tw_posts = cur.fetchall()


# In[6]:


# LAST Facebook Post
cur = engine.execute(
    '''     
    SELECT 
        to_char (dt_post::timestamp at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS') as dt
        ,msg as msg
        ,"user" as user
        ,likes as likes
        ,'facebook' as sorgente
        ,''' "'" + str(todays_date) + "'" ''' as dt_rif
    FROM ''' + schema + '''."fb_posts"
    WHERE 
        "user" in (''' "'" + user1 + "'" ''',''' "'" + user2 + "'" ''',''' "'" + user3 + "'" ''')
        and dt_rif=(select max(dt_rif) from ''' + schema + '''."fb_posts")
            ''')
fb_posts = cur.fetchall()


# In[7]:


# LAST News
cur = engine.execute(
    '''     
    SELECT distinct
        to_char ("pubAt"::timestamp at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS') as dt
        ,titolo as msg
        ,'all' as user
        ,0 as likes
        ,'news' as sorgente
        ,''' "'" + str(todays_date) + "'" ''' as dt_rif
    FROM ''' + schema + '''."news"
    WHERE 
        dt_rif=(select max(dt_rif) from ''' + schema + '''."news")
        order by to_char ("pubAt"::timestamp at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS') desc
        limit 9
            ''')
news = cur.fetchall()


# In[8]:


header = ['dt','msg','user','likes','sorgente', 'dt_rif']
df_tw_post = pd.DataFrame(tw_posts, columns=header)
df_fb_post = pd.DataFrame(fb_posts, columns=header)
df_news = pd.DataFrame(news, columns=header)


# In[9]:


df_all = df_news.append(df_tw_post).append(df_fb_post)


# In[12]:


df_all.head(2)


# In[13]:


# write to db
df_all.to_sql('timeline', engine, schema=schema, if_exists='append')

