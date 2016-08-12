import sqlalchemy
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#Here's where the database connection information goes
#format is sqlflavor://username:password@host/databasename
engine=sqlalchemy.create_engine( \
       "postgresql://username:password@localhost/databasename")

#Here we have a class that is mapped to a table schema using declarative_base()
#__init__ is already done for you, you have to specify every column 
Base=declarative_base()
class User(Base):
  __tablename__ = 'testtable'
  id  =Column(String(10),primary_key=True)
  name=Column(String(50))
  age =Column(Integer)
  def __repr__(self):
    return "<User(name='%s', age='%f')>" % (self.name,self.age)

#This uses the engine to connect to the database and create the table
Base.metadata.create_all(engine)

#This starts a session so you can add stuff to the table
Session=sessionmaker(bind=engine)
session=Session()  #a session instance (you could have more than one)

#This creates an instance of the class and stuffs it into a list
#of changes to be committed (think like git add)
someuser=User(id='al24406',name='Alexia Schulz',age=37)
session.add(someuser)

#This adds a bunch of users at once
session.add_all([User(id='ef34343',name='effram higgledy',age=95),User(id='hi121212',name='Hildegard Jones',age=22),User(id='ja124433',name='Jane Diggs',age=58)])

#This stuffs it into the database
session.commit()

#This does a quert to find names that have the substring ne in them
session.query(User).filter(User.name.like('%ne%')).all()

#This closes the session
session.close()



