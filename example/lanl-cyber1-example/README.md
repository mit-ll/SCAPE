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

* In the directory SCAPE/example/lanl-cyber1-example
  open the file lanldata2postgres.py, go to line beginning with "engine ="
  and modify this line to point to your own username, password, hostname 
  (I used localhost), port, and database name.  

* Fire up an ipython shell.  Create the tables to store the LANL data by
  running the command lanldata2postgres.create() .  Check that you succeeded
  by running the command lanldata2postgres.aretheretables().

* Pretend your LANL data files live in a directory called Flatfiles.  One by one, 
  unzip the LANL data files, and ingest them into your newly
  minted tables using this command (the flows file in this example):
  "lanldata2postgres.stuff('Flatfiles/flows.txt','flows', maxtime=3600)" .
  The time is in seconds, so this command ingests an hour.  You can use 
  mintime=3600, maxtime=7200 to get the next hour, etc.  When you are done, 
  zip each file back up.  

* Note: the auth data will take a long time to zip/unzip, it's big. 

* Now that your data is nicely ingested, it's time to play with SCAPE. First
  you must add the scape directory to your python path using this command (bash):
  "export PYTHONPATH=PathToScapeRepoHere:$PYTHONPATH"  Make sure there are not 
  spaces next to the =.  

* Change into the directory SCAPE/example/lanl-cyber1-example.  fire up an 
  ipython notebook (or jupyter notebook) by typing ipython notebook at the 
  command line (or jupyter notebook)

* To run cells in this notebook, hit shift-enter.  Try running the cells.  
  Try writing new cells and evaluating them.  There are a few more examples
  of query options in the SplunkScape.html file in this directory.  Explore
  the Pandas documentation for ideas on other data science you might try, 
  now that you have the data at your disposal.  

* That's all folks.  Please let me know if any instructions didn't work.  
  And now: an orange tictac for me.   
