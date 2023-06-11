#!/usr/bin/env python
# coding: utf-8

# In[2]:


#pip install Faker


# In[3]:


from faker import Faker


# In[4]:


#pip install pandas


# In[5]:


import pandas as pd


# In[6]:


import random


# In[7]:


import numpy as np


# In[8]:


import json


# In[9]:


fake = Faker()


# In[10]:


UserID=[]
User = []
Location = []
SessionID = []
URL = []
LogTime = []
PaymentMethod = []
LogDate = []


# In[11]:


items=['chair','Iphone 13','Laptop','shoes','watches','T-shirts','Jeans','Headphones','Iphone14','jackets','mobile cover',
      'charger','HomeTheatre','Blanket','NoteBook','Bike Cover','Garbage Bag','Pen','Ball Pen','Gel Pen','Ink Pen','Casual Shoes',
      'Trousers','Casual Shirts','Ink','Table','Boat Headphones','Belts','Tiffin Box','Hoodies','Sweat Shirts','Shorts',
      'Perfumes','Body Spray','Brush','Soap','Shampoo','Screen Guard','School Bags','Pencil','Air Conditioner','Refrigerator',
      'Washing Machines','Microwaves','Dishwasher','Chimneys','Smart Watch','Face Wash','Hand Bags']


actions=['view','removefromcart','addtocart','purchase']

payment_method=['Credit card', 'Debit card', 'UPI','Net Banking','cash on delivery']

names=['Diane Patrick','David Brown','Andrew Marshall','Megan Davidson','Margaret Clark','Mary Rodriguez',
       'Alexis Duncan','Melanie Calhoun','Marilyn Lane','Teresa Garcia MD','Cynthia Ponce','Jose Kirk',
       'Nathan Marquez','Michelle Jenkins','Jennifer Thomas','Jennifer King','Mary Hess','Kayla Howard',
       'Donald Taylor','Janice Velasquez','William Smith','Robert Davis','Joshua Marquez','Charles Brown',
       'Steven Pena','Aaron Hobbs','Karen Wright','Valerie Callahan','Javier Powell','Jennifer Dominguez PhD',
       'Terry Evans','Michael Crane','Dennis Best','Jason Ward','Kenneth Elliott','Vincent Hansen',
       'Danielle Mclean','Sean Lewis','Kristen Woods','Sharon Ellis','Caleb Roberts','Steven Miller MD',
       'Jennifer Marshall','Jennifer Carey','Roger Bradshaw','Charlotte West','Gene Griffin','Michelle Cox',
       'Melody Harris','Emily Lewis','Kelly Boyd','Michael Leon','Vanessa Luna','Daniel Brown','Michael Cisneros',
       'Robert Perez','Catherine Castro','Gerald Erickson','Shelly Peterson','Stephen Rodriguez','Nichole Cook',
       'Scott Hernandez','Jesse Vargas','Chad Brooks','Heather Dickerson','Austin Brown','Bryan Waters',
       'Jeremy Vance','Christian Ryan','Michael Marsh','Stephen Henry','Timothy Hall','Joe Lowe','Kevin Herrera',
       'Jacob Dunn','Jamie Sims','Michael Roberts','Mr. Michael Cross','Chad Little','James Hernandez',
       'Kyle Miller','Jason Williams','Dalton Waller','Christine Davis','Erin Mathis','Shirley Scott',
       'Matthew Berry','Anthony Macdonald','Kim Hampton','Crystal Williams','Danielle Palmer','Harold Alexander',
       'Gregory Cuevas','Darren Bradley','Christopher Will']

location=['Stefaniestad','Perezburgh','Adambury','Porterview','New Robertshire','Port Jameshaven','Tonichester',
          'Marytown','Carrollton', 'South Alexport','Williamborough','New Luisside','Charlottetown','Kennedyton',
          'Brandonbury','Jamesstad','West Juliafurt','New Thomasmouth','New Katieland','South Christina','East Richardmouth',
          'Lake Dominiqueshire','South Christina','Lake Timothy','Glennhaven', 'West Allisonville','Port Kevin',
          'Jenkinschester', 'New Phillipmouth','Troymouth','South Michael','Gonzalestown','East Nicholasborough',
          'Susanland', 'Chadton','Stevenstad','New David', 'New Kayla', 'New Jamesfort', 'Laurenburgh',
          'Russellview', 'Pamelaland', 'Daniellehaven',]


# In[12]:



def dummy_data(numRec):
    
    for _ in range(numRec):
        
        logdate=fake.date_this_year().isoformat()
        logtime=fake.time() 
        
        # fake UserId
        
        UserID.append(fake.bothify('???-#####'))
        #UserID.append(np.random.shuffle(userid))
        
        # fake name
        User_Name.append(np.random.choice(names))
        
        # fake city
        Location.append(np.random.choice(location))
        
        # fake session ID
        SessionID.append(fake.bothify('Session_clickShop-##############'))
        
        # fake URL
        items_list = random.choice(items)
        action_list= random.choice(actions)
        url = "http:www.shop.com."+str(action_list)+".item." + str(items_list)
        URL.append(url)   
        
        # Fake action
        Action.append(action_list)
        
        # Fake Log Date
        logtime=logdate+" "+logtime
        LogTime.append(logtime)
        
        # Fake Payment Method
        if action_list=='purchase':
            PaymentMethod.append(np.random.choice(payment_method))
            
        else:
            PaymentMethod.append("-")
        #payment_method=['Credit card', 'Debit card', 'UPI','Net Banking','cash on delivery']
                 
        # Fake logdate
        LogDate.append(logdate)


# In[13]:


import json
from json import loads, dumps
UserID=[]
User_Name = []
Location = []
SessionID = []
URL = []
Action = []
LogTime = []
PaymentMethod = []
LogDate = []

dummy_data(100000)
df = pd.DataFrame(zip(UserID,User_Name,SessionID,URL,Action,LogTime,PaymentMethod,LogDate,Location),columns=['UserID','User_Name','SessionID','URL','Action','LogTime','PaymentMethod','LogDate','Location'])
#result=df.to_csv('/home/suraj/Data.csv', index=False)
result=df.to_json('/home/suraj/Data.json', orient='records', lines='true' )
#parsed=json.loads(result)
#json.dumps(parsed)


# In[ ]:




