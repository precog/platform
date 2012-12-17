Data Science on MongoDB...At Last!
==================================

Today, I’m excited to announce the launch of Precog for MongoDB, a
release that bundles all of the really cool Precog technology into a
free package that anyone can download and deploy on their existing
MongoDB database.


We provide a cloud-hosted version of Precog, but we’ve known for a
long time that we were going to bring a standalone version of our data
science Precog to some NoSQL database. Precog is a data science
platform that lets developers and data scientists do advanced
analytics and statistics using Quirrel, the “R for big data”
language. You can analyze data programmatically with a REST API (or
client library) or interactively with Labcoat, an easy-to-use HTML5
application built on the REST API.


MongoDB makes the perfect choice for many reasons:


 1. MongoDB developers share our passion for creating software that
developers love to use.  

 2. Quirrel is designed to analyze JSON, which is natively supported by
MongoDB.

 3. MongoDB has a basic query and aggregation framework, but to do more
advanced analytics, you have to write lots of custom code or export
the data into a RDBMS, both of which are very painful.

 4. We’re great friends of some of the 10gen developers and have
released open source software for MongoDB.


Precog for MongoDB gives you the ability to analyze all the data in
your MongoDB database, without forcing you to export data into another
tool or write any custom code.


We’re really excited about the release and encourage you to download
the release from the official product page and start using it today.


In the remainder of this post, I’m going to quickly walk you through
installation and configuration of the Precog for MongoDB release.


Step 1: Unpack the Download
---------------------------

The download is a ZIP file that contains the following files:

 * precog.jar
 * config.cfg
 * precog.sh
 * precog.bat

The file **precog.jar** is the Java JAR that bundles all of the Precog
dependencies into a single (really big!) file. The files **precog.sh** and
**precog.bat** are scripts that launch **precog.jar**.


The file **config.cfg** contains configuration information.


Step 2: Configure Precog
------------------------

All the configuration settings for Precog are stored in the file
**config.cfg**, with reasonable defaults.


There are two things you need to do at a minimum before you can launch
Precog:


 1. Tell Precog where to find the MongoDB server. 

 2. Tell Precog what the master account is.


To tell Precog where to find the MongoDB server, simply edit the mongo
server key in the settings:

    queryExecutor { 
        mongo { 
            server = "mongodb://localhost:27017" 
        } 
    }

Change the “localhost:27017” portion to the host and port of your
mongo server. For optimal performance, you should launch Precog on the
same machine that is running the MongoDB server.

You can also add a dbAuth section under the mongo section that allows
you to set user/pass info for particular databases:

    queryExecutor { 
        mongo { 
            server = "mongodb://localhost:27017" 
            dbAuth {
                test = "user:pass"
                test2 = "user:pass2"
            }
        } 
    }


Precog will map the MongoDB databases and collections into the file
system by placing the databases at the top level of the file system,
and will nest the database collections under the databases
(e.g. */mydb/mycollection/*).


To tell Precog what the master account is, edit **config.cfg** and edit
the following settings under the masterAccount section:


    security { 
        masterAccount { 
            apiKey = "12345678-1234-1234-1234-123456789abc"
        }
    }


The API key for the master account can be anything you like, but you
should treat it securely because whoever has it has full access to all
of your MongoDB data.


You may also want to tweak the ports that Precog uses for the web
server that exposes the Precog REST API and to serve labcoat:


    server { 
        port = 8888 
    } 
    ...  
    labcoat { 
        port = 8000 
    }


Step 3: Launch Precog
---------------------

To run **precog.jar**, you will need to install JRE 6 or later (many
systems already have Java installed). If you’re on an OS X or Linux
machine, just run the **precog.sh** script, which automatically
launches Java. If you’re on a Windows machine, you can launch Precog
with the **precog.bat** script.


Once Precog has been launched, it will start a web server that exposes
the Analytics REST API as well as labcoat.


Step 4: Try the API
-------------------

Once Precog is running, you have full access to the Precog Analytics REST
API. However, you will need to load data into MongoDB using MongoDB's API.  That is, you cannot use the Precog Ingest API to load data into MongoDB because Precog for MongoDB does not bundle the auth, accounts or ingest services. To obtain access to the rest of the Precog REST API you will need to sign-up for an account and use the cloud-based service. You can find a large number of open source client libraries
available on Github, and the Precog developers site contains documentation and tutorials for interacting with the API.


Step 5: Try Labcoat
-------------------

Labcoat is an HTML5 application that comes bundled in the
download. You don’t have to use Labcoat, of course, since Precog has a
REST API, but Labcoat is the best way to interactively explore your
data and develop Quirrel queries.


The precog.jar comes with a bundled web server for labcoat, so once
it’s running just point your browser at http://localhost:8000/ (or
whatever port you’ve configured it for) and you’ll have a new labcoat
IDE pointing at your local Precog REST API.


Step 6: Analyze Data!
---------------------

Once you’ve got Labcoat running, you’re all set! You should see your
MongoDB collections in the file system explorer, and you can query
data from the collections, develop queries to analyze the data, and
export queries as code that run against your Precog server.


Precog is a beta product, and *Precog for MongoDB* is hot off the
press. You may encounter a few rough corners, and if so, we’d love to
hear about them (just send an email to support@precog.com).

Precog for MongoDB is a free product that Precog provides to the MongoDB community for doing data analysis on MongoDB.

Due to technical limitations, we only recommend the product for exploratory data analysis. For developers interested in high-performance analytics on their MongoDB data, we recommend our cloud-based analytics solution and the MongoDB data importer, which can nicely complement existing MongoDB installations for analytic-intensive workloads.

If you end up doing something cool with Precog for MongoDB, or if you
just want to say hello, feel free to reach out to us via our website,
or to me personally at john@precog.com.


Have fun analyzing!


John A. De Goes, CEO/Founder of Precog
