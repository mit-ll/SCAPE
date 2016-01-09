Scalable Cyber Analytic Processing Environment (SCAPE)
=======================================================

The basic problem Scape tries to solve is this: given some number of
data sources from network sensors (router netflow, web proxy, IDS
alerts, etc.), how to immediately begin ingesting the data into
long-term scalable storage while providing for continuous knowledge
engineering that directly informs downstream query and analysis?

To this end, Scape provides data storage, knowledge engineering,
query, analysis and visualization tools to service the situational
awareness needs of network defenders and cyber analysts.

Features
--------

* Schemas and frameworks for ingestion into
  [Accumulo](https://accumulo.apache.org/), a scalable BigTable-like
  NoSQL storage architecture

* Knowledge Registry -- a knowledge engineering (KE) framework for
  describing data sources using domain-specific data types and
  descriptive tags

* Python DSL -- a query construction API for ad hoc data exploration,
  scalable analytics and visualization development. Leverages the KE
  information in the Knowledge Registry to provide a domain-specific
  view of the data

* User-specific configuration of Scape environment 

* Various parsers and helper utilities for dealing with cyber data


References
----------

* David O'Gwynn, et al. "SCAPE: A Scalable Cyber Analytic Processing
  Environment", Unpublished. Forthcoming.

----------
Installing
----------

### Setting up Python

##### Python distribution

It is possible to use the Python distribution that comes with your
system (if you're on Mac OSX or Linux), but we suggest installing one
of the scientific-computing-oriented Python distros. E.g.:

* [Enthought Canopy](https://store.enthought.com/downloads/): This is
  what I (David O'Gwynn) use. Had good success with it so far.

* [Continuum Anaconda](https://store.continuum.io/cshop/anaconda/):
  Have not tried, but comes with most of the same kinds of packages as
  the Canopy distro.

Both of these distros can be installed in user-space (or
system-wide). We suggest using a separate distro rather than the
system distro for a number of reasons:

* Potentially dangerous to monkey with the system Python, as it might
  be in use for OS-related tasks.

* Somewhat difficult to correctly build some of the 3rd-party
  scientific packages (e.g. SciPy).

* Just easier to do research with a Python distro that you control

##### Acqua

The primary module that Scape depends on is the
Acqua Java library. The installation instructions for Acqua will describe how to install the
[JCC module](https://pypi.python.org/pypi/JCC/), the [Apache Lucene
project's](http://lucene.apache.org/pylucene/jcc/index.html) C++ code
generator for calling Java from C++/Python via JNI bindings.

##### Third-party module dependencies

Scape has a number of extra 3rd-party module dependencies. These are
located in the dependencies subdirectory. There are instructions there for installing them. 

### Setting up Accumulo

Scape requires an Accumulo backend. Currently, it supports three
connection methods:

* an in-memory MockAccumulo instance (default) -- Only recommended for
  simple usage scenarios (unit tests, etc.). We use this for our
  tutorial example

* a file-system-based MiniAccumuloCluster instance -- The Accumulo
  community's preferred unit-test instance, but it is a little slower
  to load

* an existing Accumulo **1.5x** stack -- Required for actual
  scalability

If you plan on ingesting data of significant size (>=O(1M)), or if you
plan on using Scape's Spark support, we suggest creating at least a
single-node Accumulo instance. There are [instructions for doing
so](https://accumulo.apache.org/1.5/accumulo_user_manual.html#_installation)
on Accumulo's [Apache site](https://accumulo.apache.org/).

---------------
Getting Started
---------------

A Scape deployment is composed of a few things:

1. Setup of Accumulo and Python environment

1. Data sources on the file system in semi-structured form (currently
   CSV; JSON, XML in future releases)

1. Knowledge Registry definitions for these sources

1. Environment configuration for Accumulo, Python and Knowledge
   Registry for the given data sources




