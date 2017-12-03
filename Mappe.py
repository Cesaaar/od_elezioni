
# coding: utf-8

# In[52]:


# Import Librerie
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import nltk
from nltk.tokenize import word_tokenize
import re
from collections import Counter
from nltk.corpus import stopwords
import string
import datetime

config = {}
config_path = os.path.join(os.path.abspath('../../'))
config_name = 'config.py'
config_file = os.path.join(config_path,config_name)
exec(open(config_file).read(),config)


# In[2]:


# get database connection
db=config['DATABASE_ELE']
schema=config['SCHEMA_ELE']
engine = create_engine(db)

user1 = config['USER1']
user2 = config['USER2']
user3 = config['USER3']


# In[3]:


#Leggo il file messo a disposizione da Istat per i comuni
url="http://www.istat.it/storage/codici-unita-amministrative/Elenco-comuni-italiani.csv"
df_comuni = pd.read_csv(url,sep = ';',encoding='latin-1')


# In[73]:


comuni = df_comuni[u'Denominazione in italiano']


# In[53]:


# get today's date
todays_date = datetime.datetime.now()


# In[9]:


# Leggo le ultime news
cur = engine.execute(
    '''     
    SELECT
    "desc" as msg
    ,to_char ("pubAt"::timestamp at time zone 'UTC', 'YYYY-MM-DD') as dt
    ,fonte as fonte
    ,"user" as user
    FROM ''' + schema + '''."news"
    WHERE dt_rif=(select max(dt_rif) from ''' + schema + '''."news")
    ''')
f_news = cur.fetchall()
header = ['msg','dt','fonte','user']
df_news = pd.DataFrame(f_news, columns=header)


# In[8]:


# Leggo gli ultime social
cur = engine.execute(
    '''     
    SELECT
    msg as msg
    ,to_char(dt::timestamp, 'YYYY-MM-DD') as dt
    ,sorgente as fonte
    ,"user" as user
    FROM ''' + schema + '''."timeline"
    WHERE 
    sorgente in ('twitter','facebook') and
    dt_rif=(select max(dt_rif) from ''' + schema + '''."timeline")
    ''')
f_social = cur.fetchall()
header = ['msg','dt','fonte','user']
df_social = pd.DataFrame(f_social, columns=header)


# In[56]:


df = df_social.append(df_news)
df.reset_index(drop=True, inplace=True)
df.head(2)


# In[57]:


emoticons_str = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""
 
regex_str = [
    emoticons_str,
    r'<[^>]+>', # HTML tags
    r'(?:@[\w_]+)', # @-mentions
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs
    r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
    r'(?:[\w_]+)', # other words
    r'(?:\S)', # anything else
    
]
    
tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)
 
def tokenize(s):
    return tokens_re.findall(s)
 
def preprocess(s, lowercase=False):
    tokens = tokenize(s)
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
    return tokens


# In[112]:


punctuation = list(string.punctuation)
stop = punctuation
mappe = []
for i, row in df['msg'].iteritems():
    terms_only = [term for term in preprocess(row) if term not in stop and not term.startswith((':/'))]
    for term in terms_only:
        d = {}
        # check comune
        if not(comuni[comuni.isin([term])].empty):
            d['comune'] = term
            d['fonte'] = df['fonte'][i]
            d['dt_post'] = df['dt'][i]
            d['user'] = df['user'][i]
            d['dt_rif'] = todays_date
            mappe.append(d)


# In[113]:


df_mappe = pd.DataFrame(mappe)


# In[114]:


df_mappe.head(2)


# In[115]:


# write to db
df_mappe.to_sql('mappe', engine, schema=schema, if_exists='append')

