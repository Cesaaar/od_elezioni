
# coding: utf-8

# In[148]:



import sys  
reload(sys)  
sys.setdefaultencoding('utf-8')

import pandas as pd 
import numpy as np 
import datetime as dt 
from sqlalchemy import create_engine 
import os
from os import path
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from calendar import monthrange
from pandas.tseries.offsets import *

import nltk
from nltk.tokenize import word_tokenize
import re
from collections import Counter
from nltk.corpus import stopwords
import string

import json
import random


# In[149]:


config = {}
config_path = os.path.join(os.path.abspath('../../'))
config_name = 'config.py'
config_file = os.path.join(config_path,config_name)
exec(open(config_file).read(),config)

# get database connection
db=config['DATABASE_ELE']
schema=config['SCHEMA_ELE']
engine = create_engine(db)


# In[150]:


# collezioni i testi dal database
cur = engine.execute(
    '''
    select 
    "user" as utente
    ,titolo as msg
    ,max(to_char(dt_rif ,'YYYY-MM-DD')) as dt_rif
    from ''' + schema + '''."news"
    where "user" != 'all'
    group by "user",titolo

    UNION ALL

    select 
    "user" as utente
    ,msg as msg
    ,max(substring(dt_rif from 0 for 11)) as dt_rif
    from ''' + schema + '''."timeline"
    where "user" != 'all'
    group by "user",msg
    
    '''
)

f_word = cur.fetchall()
header = ['utente','msg','dt']
df_word = pd.DataFrame(f_word, columns=header)


# In[151]:


df_word.head(2)


# In[152]:


stopwords = [':/','0','di','e','il','la','E',
'a','DI','per','IL','in','non','che','i','A','del','un','con','CHE','LA','ha','le','La',
'si','da','Il','ma','su','dei','IN','I', 'NON','UN','ci','PER','UNA','HA','L','PD',
'Pd','l','DEL','Non','CON','SI','sono','noi','DELLA','pi','Ma','DA','chi','come','senza',
'questo','fa','solo','o','MA','Che','Di','DOPO','dopo','mi','5','soli','ho','questa','se',
'4','Per','tra','SOLO','cosa','GLI','NEL','nella','nel','gli','O','CHI','questi','O','gli',
'SU','c','cui','dove','tutti','qualit','tutto','queste','degli','gi','D','COME','C','SE',
'DEI','POI','vuole','FATTO','al','ancora','https','anche','lo','loro','delle','alla',
'abbiamo','co','alle']


# In[153]:


df_word_renzi = df_word[df_word['utente']=='Renzi']
df_word_dimaio = df_word[df_word['utente']=='Di Maio']
df_word_berlusconi = df_word[df_word['utente']=='Berlusconi']


# In[154]:


df_word_renzi['msg'].to_csv('renzi.txt')
df_word_dimaio['msg'].to_csv('dimaio.txt')
df_word_berlusconi['msg'].to_csv('berlusconi.txt')


# In[155]:


text_renzi = open('renzi.txt').read()
text_dimaio = open('dimaio.txt').read()
text_berlusconi = open('berlusconi.txt').read()


# In[161]:


def grey_color_func(word, font_size, position, orientation, random_state=None,
                    **kwargs):
    return "hsl(240, 0%%, %d%%)" % random.randint(10, 40)


# In[162]:


# Generate a word cloud image
wordcloud_renzi = WordCloud(stopwords=stopwords, background_color='white').generate(text_renzi)
wordcloud_dimaio = WordCloud(stopwords=stopwords, background_color='white').generate(text_dimaio)
wordcloud_berlusconi = WordCloud(stopwords=stopwords, background_color='white').generate(text_berlusconi)


# In[163]:


# store default colored image
default_colors_renzi = wordcloud_renzi.to_array()
wordcloud_renzi.recolor(color_func=grey_color_func, random_state=3)


# In[164]:


# store default colored image
default_colors_dimaio = wordcloud_dimaio.to_array()
wordcloud_dimaio.recolor(color_func=grey_color_func, random_state=3)


# In[165]:


# store default colored image
default_colors_berlusconi = wordcloud_berlusconi.to_array()
wordcloud_berlusconi.recolor(color_func=grey_color_func, random_state=3)


# In[172]:


dir_out = os.path.join('/home/pi/Documents/Progetti/elezioni/elezioni/static/assets/','images')


# In[173]:


wordcloud_renzi.to_file(os.path.join(dir_out,'wordcloud_renzi.png'))
wordcloud_dimaio.to_file(os.path.join(dir_out,'wordcloud_dimaio.png'))
wordcloud_berlusconi.to_file(os.path.join(dir_out,'wordcloud_berlusconi.png'))

