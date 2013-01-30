Precog for PostgreSQL 
=====================

Precog for PostgreSQL is a free release that swaps out Precog's native data
store for PostgreSQL, effectively using PostgreSQL as a disk drive from which 
data is loaded. The data is translated on-the-fly into a format that Precog
can work with.


Precog for PostgreSQL gives you the ability to analyze all the data in
your PostgreSQL database, without forcing you to export data into another
tool or write any custom code.

The following sections contain a short guide for getting started with Precog 
for PostgreSQL. You'll be up and running in minutes!


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


 1. Tell Precog the location of your database(s)

 2. Tell Precog what the master account is.


To tell Precog the location of the database(s), simply edit the
databases section under queryExecutor:

    queryExecutor { 
        databases { 
            local = "jdbc:postgresql://localhost/test" 
        } 
    }

The format of each line is

    <database name> = <JDBC PostgreSQL URL>

Where <database name> is whatever you want the database to be named
*within Quirrel*, and the URL is of the form
`jdbc:postgresql://<host>/<postgresql database name>` (see [the
PostgreSQL JDBC
reference](http://jdbc.postgresql.org/documentation/91/connect.html)
for more details on the syntax).

For optimal performance, you should
launch Precog on the same machine that is running the PostgreSQL server.


To tell Precog what the master account is, edit **config.cfg** and edit
the following settings under the masterAccount section:


    security { 
        masterAccount { 
            apiKey = "12345678-1234-1234-1234-123456789abc"
        }
    }


The API key for the master account can be anything you like, but you
should treat it securely because whoever has it has full access to
your database.


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
API. However, you will need to load data into PostgreSQL using standard SQL 
insertion outside of Precog.  That is, you cannot use the Precog Ingest API to
load data into PostgreSQL because Precog for PostgreSQL does not bundle the 
auth, accounts or ingest services. To obtain access to the rest of the Precog 
REST API you will need to sign-up for an account and use the cloud-based 
service. You can find a large number of open source client libraries available 
on Github, and the Precog developers site contains documentation and tutorials 
for interacting with the API.


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
PostgreSQL databases and tables in the file system explorer, and you can query
data from the tables, develop queries to analyze the data, and
export queries as code that run against your Precog server.


Precog is a beta product, and *Precog for PostgreSQL* is hot off the
press. You may encounter a few rough corners, and if so, we’d love to
hear about them (just send an email to support@precog.com).

Due to technical limitations, we only recommend the product for exploratory 
data analysis. For developers interested in high-performance analytics on their 
PostgreSQL data, we recommend our cloud-based analytics solution and the 
PostgreSQL data importer, which can nicely complement existing PostgreSQL 
installations for analytic-intensive workloads.

Have fun analyzing!