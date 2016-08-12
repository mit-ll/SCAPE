"""
   NAME:
      lanldata2postgres
   PURPOSE:
      Put the Los Alamos dataset into a postgres database using sqlalchemy
   REVISION HISTORY:
      23 May 2016  -  Alexia
      03 Aug 2016  -  Added column type introspection, check for existing data
      04 Aug 2016  -  Added proc, flows, dns, redteam classes

"""
import time
import sqlalchemy
from sqlalchemy import Column, Integer, String, Numeric, DATETIME
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

####################################################################
# Methods for connecting to database
####################################################################

# Here's where the database connection information goes
# format is sqlflavor://username:password@host:port/databasename

global engine
engine=sqlalchemy.create_engine( \
       "postgresql://insertusername:insertpassword@localhost:5432/lanldb")

def setengine(newengine):
  """reset the global engine to point somewhere else"""
  global engine
  engine=newengine
  return

####################################################################
# Classes for each LANL datastream
####################################################################

# Each class is mapped to a table schema using declarative_base()
# __init__ is already done for you, you have to specify every column

Base=declarative_base()

class auth(Base):
  """Authentication data"""
  __tablename__='auth'
  id         = Column(Integer,primary_key=True)
  time       = Column(Integer,nullable=False)
  suser      = Column(String(50),nullable=True)
  duser      = Column(String(50),nullable=True)
  shost      = Column(String(50),nullable=True)
  dhost      = Column(String(50),nullable=True)
  authtype   = Column(String(50),nullable=True)
  logontype  = Column(String(50),nullable=True)
  authorient = Column(String(50),nullable=True)
  status     = Column(String(50),nullable=True)
  
  def __repr__(self):
    return "id='%d', time='%d', suser='%s', duser='%s', " \
           % (self.id, self.time, self.suser, self.duser) + \
           "shost='%s', dhost='%s', authtype='%s', " \
           % (self.shost, self.dhost, self.authtype) + \
           "logontype='%s', authorient='%s', status='%s'" \
           % (self.logontype, self.authorient, self.status)
  
class proc(Base):
  """Process start and stop events collected from Windows computers and servers"""
  __tablename__='proc'
  id         = Column(Integer,primary_key=True)
  time       = Column(Integer,nullable=False)
  user       = Column(String(50),nullable=True)
  host       = Column(String(50),nullable=True)
  procname   = Column(String(50),nullable=True)
  startstop  = Column(String(50),nullable=True)

  def __repr__(self):
    return "id='%d', time='%d', user='%s', host='%s', procname='%s', startstop='%s'"\
            % (self.id, self.time, self.user, self.host, self.procname, self.startstop)

class flows(Base):
  """Network flow events collected from central routers"""
  __tablename__='flows'
  id         = Column(Integer,primary_key=True)
  time       = Column(Integer,nullable=False)
  duration   = Column(Integer,nullable=False)
  shost      = Column(String(50),nullable=True)
  sport      = Column(String(50),nullable=True)
  dhost      = Column(String(50),nullable=True)
  dport      = Column(String(50),nullable=True)
  protocol   = Column(String(50),nullable=True)
  pktcount   = Column(Integer,nullable=False)
  bytecount  = Column(Integer,nullable=False)

  def __repr__(self):
    return "id='%d', time='%d', duration='%d', shost='%s', " \
         % (self.id, self.time, self.duration, self.shost) + \
         "sport='%s', dhost='%s', dport='%s', " \
         % (self.sport, self.dhost, self.dport) + \
         "protocol='%s', pktcount='%d', bytecount='%d'" \
         % (self.protocol, self.pktcount, self.bytecount)

class dns(Base):
  """Domain Name Service (DNS) lookup events"""
  __tablename__='dns'
  id         = Column(Integer,primary_key=True)
  time       = Column(Integer,nullable=False)
  shost      = Column(String(50),nullable=True)
  resolvedhost = Column(String(50),nullable=True)

  def __repr__(self):
    return "id='%d', time='%d', shost='%s', resolvedhost='%d'" \
           % (self.id, self.time, self.shost, self.resolvedhost)

class redteam(Base):
  """Authentication events that present known redteam compromise events"""
  __tablename__='redteam'
  id         = Column(Integer,primary_key=True)
  time       = Column(Integer,nullable=False)
  user       = Column(String(50),nullable=True)
  shost      = Column(String(50),nullable=True)
  dhost      = Column(String(50),nullable=True)

  def __repr__(self):
    return "id='%d', time='%d', user='%s%, shost='%s', dhost='%s'" \
           % (self.id, self.time, self.user, self.shost, self.dhost)

####################################################################
# Helper functions to navigate and introspect the tables
####################################################################
tabledict={'auth':auth, 'proc':proc, 'flows':flows, 'dns':dns, 'redteam':redteam}

# This helps ingest the right data type.  Eventually we will want to figure 
# out the sqlalchemy type decorator or sqlalchemy.sql.expression.cast to do this 
# more elegantly.
typedict={Integer:int,Numeric:float}
 
def find_type(class_, colname):
  """Helps introspect types for columns, will recursively descend in case of inheritance."""
  if hasattr(class_, '__table__') and colname in class_.__table__.c:
    return class_.__table__.c[colname].type
  for base in class_.__bases__:
    return find_type(base, colname)
  raise NameError(colname)

####################################################################
# Create and destroy tables, and ingest CSV data
####################################################################

def create():
  """create necessary tables"""
  Base.metadata.create_all(engine)
  return

def aretheretables():
  """Return list of existing tables"""
  return [i.name for i in Base.metadata.tables.values()]

def destroy():
  """drop tables in declarative base"""
  # I do not understand why we need this but apparantly commiting
  # nothing makes it possible to remove various transaction locks
  # that prevent the tables from getting dropped.  Creepy. 
  Session=sessionmaker(bind=engine)
  session=Session()
  session.commit()

  # Here is where we actually drop the tables
  Base.metadata.drop_all(engine)

  session.close()
  return

def stuff(datasourcefile,tablename,mintime=0, maxtime=20):
  """stuff data from CSV into postgres tables"""
  t0=time.time()

  #connect up a database session
  Session=sessionmaker(bind=engine)
  session=Session()

  # pick the right table and grab the column headers
  whichtable = tabledict[tablename]
  thesekeys  = whichtable.metadata.tables[tablename].columns.keys()

  # Figure out if there is data in the table, and find the largest uid
  lastrow = session.query(whichtable).order_by(whichtable.id.desc()).first()
  try:
    uid= lastrow.id
    uid=int(uid)+1
  except:
    uid=0
  print 'Starting from uid: ',uid

  #iterate through CSV file and ingest data between mintime and maxtime
  with open(datasourcefile,'r') as fp:
    #uid=0
    for line in fp:
        tokens=line.split(',')
        if long(tokens[0]) > maxtime: break
        if long(tokens[0]) >= mintime: 
           tokens.insert(0,uid)
           #Recast non-string columns to the correct data type
           for i in range(len(thesekeys)):
             columntype=type(find_type(whichtable,thesekeys[i]))
             if columntype != String:
               tokens[i]=typedict[columntype](tokens[i])
             else:
               if tokens[i][-1]=='\n':
                 tokens[i]=tokens[i][:-1]

           kwargs = dict(zip(thesekeys,tokens))
           thisevent=whichtable(**kwargs)
           session.add(thisevent)
           uid+=1 #increment unique id number
  
  #stuff into database
  session.commit()
  session.close()

  print 'Data from',mintime,' to ',maxtime, ' took ', (time.time()-t0)/60., 'Minutes.'
  return
          

