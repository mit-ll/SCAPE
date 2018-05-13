SCAPE Example Using Los Alamos Public Cyber Dataset
====================================================

This directory contains materials needed to set up a 
working SCAPE example in your own environment.  This 
example works with a SQL back end data store.  It has
been tested with Postgres, but is implemented with 
SQLAlchemy, so should accommodate your favorite SQL
flavor.  

When unzipped, the LANL dataset is reasonably large.  This example is
set up so that you can experiment on a laptop, by only ingesting a
small fraction of the data.  Note that the decompressed auth.txt.gz
file needs 68 G of free space and storage in the database is
comparable. 

Step by Step Setup Instructions
-------------------------------

* Install the anaconda distribution of python, if you have no python.
  You may need to execute "conda install psycopg2" if you run
  into issues later (this will be apparent from the traceback). 

* Install [Postgres](https://www.postgresql.org/download/). Scape uses
  SQLAlchemy so many databases are supported, but we use Postgres for
  this example as it makes the ingestion of CSVs simple.

* Make note of which port your database will be served over, for Postgres
  it commonly appears on port 5432, but you can find out by typing 
  "ps -ef | grep postgres" and locating the pgagent, and reading off the
  port.  

* Create a user account and give it privilages.  If convenient, make
  your user account name match your unix account name (or whatever the
  windows equivalent of that statement may be).  Select a non-sensative 
  password because you may choose to store it in a flat file later.

* Download a copy of the Los Alamos dataset.  
  [LANL Dataset](http://csr.lanl.gov/data/cyber1/)

* Modify the ingest_cyber1.sql file to include the path of the dataset
  and optionally number of records to ingest.

* Run `psql -U <username> -f ingest_cyber1.sql`

* Now that your data is nicely ingested, it's time to play with SCAPE. First
  you must add the scape directory to your python path using this command (bash):
  "export PYTHONPATH=PathToScapeRepoHere:$PYTHONPATH"  Make sure there are not 
  spaces next to the =.  

* Change into the directory SCAPE/example/lanl-cyber1-example.  Fire up the 
  ipython notebook "ScapeOnLanlData.ipynb"  (or jupyter notebook) 
  by typing `jupyter notebook ScapeOnLanlData.ipynb` at the command line 
  (or jupyter notebook) 

* To run cells in this notebook, hit shift-enter.  Try running the cells.  
  Try writing new cells and evaluating them.  There are a few more examples
  of query options in the SplunkScape.html file in this directory.  Explore
  the Pandas documentation for ideas on other data science you might try, 
  now that you have the data at your disposal.  

* That's all folks.  Please let me know if any instructions didn't work.  
  And now: an orange tictac for me.   
