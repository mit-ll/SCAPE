SCAPE Example Using Los Alamos Public Cyber Dataset
====================================================

This directory contains materials needed to set up a 
working SCAPE example in your own environment.  This 
example works with a SQL back end data store.  It has
been tested with Postgres, but is implemented with 
SQLAlchemy, so should accommodate your favorite SQL
flavor.  

When unzipped, the LANL dataset is reasonably large.  
This example is set up so that you can experiment on 
a laptop, by only ingesting a small fraction of the
data.  However, to unzip the auth.txt file in order 
to ingest some, you will need 68 G of free space. The
other files are smaller, and you can omit auth.txt 
from your ingested data sources if you do not have 
the available space.  

Step by Step Setup Instructions
-------------------------------

* Install the anaconda distribution of python, if you have no python.
  You may need to execute "conda install psycopg2" if you run
  into issues later (this will be apparent from the traceback). 

* Download a copy of the Los Alamos dataset.  
  [LANL Dataset](http://csr.lanl.gov/data/cyber1/)

* Install Postgres (or other sql backend): 
  [Postgres for Mac](https://www.postgresql.org/download/macosx/) or 
  [Postgres for Windows](https://www.postgresql.org/download/windows/)

* Make note of which port your database will be served over, for Postgres
  it commonly appears on port 5432, but you can find out by typing 
  "ps -ef | grep postgres" and locating the pgagent, and reading off the
  port.  

* Create a user account and give it privilages.  If convenient, make
  your user account name match your unix account name (or whatever the
  windows equivalent of that statement may be).  Select a non-sensative 
  password because you may choose to store it in a flat file later.

* Create an empty database to house your LANL data (I called mine lanldb).

* In the directory SCAPE/example/lanl-cyber1-example,
  open the file lanldata2postgres.py, find the line 
  myurl = sqlalchemy.engine.url.URL('postgresql',
                                     username='########',
                                     password="########",
                                     host="localhost",
                                     database="lanldb","
                                     port=5432)
  and modify this line to point to your own username, password, hostname 
  (I used localhost), port, and database type.  

* Open python session from this directory and type
  import lanldata2postgres
   or an ipython notebook and type
   %load lanldata2postgres.py

* Pretend your LANL data files live in a directory called Flatfiles. If you
  are able to unzip all of the files (auth.txt, dns.txt, flows.txt, proc.txt,
  redteam.txt), then call:
  > lanldata2postgres.stuffallLANLdata('Flatfiles',  maxtime=3600) 
  to ingest the first hour of data (time is in seconds). Later, you can add 
  mintime=3600, maxtime=7200 to get the next hour, etc.  
   
  If you cannot unzip all of the files at the same time due to space limitations,
  then, for each unzipped file, call
  "lanldata2postgres.stuff('Flatfiles/flows.txt','flows', maxtime=3600)" .
  When you are done, zip each file back up and move to the next one. 
  
  You can check whether you had success by calling
  > lanldata2postgres.printFirstRows()
  to see the first row in each database. If the files are still unzipped, you
  can compare these rows to the first row of text in the files by calling 
  > lanldata2postgres.printFirstRows('Flatfiles')

* Note: the auth data will take a long time to zip/unzip; it's big. 

* Now that your data is nicely ingested, it's time to play with SCAPE. First
  you must add the scape directory to your python path using this command (bash):
  "export PYTHONPATH=PathToScapeRepoHere:$PYTHONPATH"  Make sure there are not 
  spaces next to the =.  

* Change into the directory SCAPE/example/lanl-cyber1-example.  Fire up the 
  ipython notebook "ScapeOnLanlData.ipynb"  (or jupyter notebook) 
  by typing "ipython notebook ScapeOnLanlData.ipynb" at the command line 
  (or jupyter notebook) 

* To run cells in this notebook, hit shift-enter.  Try running the cells.  
  Try writing new cells and evaluating them.  There are a few more examples
  of query options in the SplunkScape.html file in this directory.  Explore
  the Pandas documentation for ideas on other data science you might try, 
  now that you have the data at your disposal.  

* That's all folks.  Please let me know if any instructions didn't work.  
  And now: an orange tictac for me.   
