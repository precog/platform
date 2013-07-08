# Precog Platform

This distribution contains the following:

* A set of JSON files that contain your account token information
* Versions 2.7.16 and 2.8.0-RC1 of the Precog platform, bundled as jar files
* Shell scripts which will allow you to:
** Configure your platform instance
** Run your platform server
** Extract your data from the Precog Cloud database
** Upload the extracted data to your private Precog Platform server(s)

## Basic Server Specifications

The Precog Platform server does not require any particularly exotic hardware to
run, but it is relatively memory-hungry, and so the more memory that can be
dedicated to its use, the better. For our internal development server, which
should serve well as a template for a single-tenant instance, we use a 2x Xeon
E5645 with 64GB of RAM and 400GB of SSD in a RAID10 config. The SSD is probably
not essential, because the platform has been recently optimized to make better
use of linear reads, and also because on our dev and test systems some
requirements of our build required a lot of disk access. 

## Prerequisites

The only piece of third-party software required to run the Precog Platform is a
standard installation of MongoDB, version 2.4 or higher. The Precog Plaform
*may* work with some earlier versions, but 2.4.3 is recommended if possible.

For highly-available systems, we also recommend using haproxy for failover and
load balancing between redundant ingest services; however, configuration of
haproxy is beyond the scope of this document. Please contact support@precog.com
for a sample configuration.

## Setup

Essentially all setup tasks required are automated by the setup.sh shell
script. To set up your environment, first edit the `config` file to ensure that
the setup script will be able to find your mongodb installation and configure
the ports on which you wish for the Precog platform services to run. Then,
simply run

    ./setup.sh

This will perform the following actions:

* Create a work directory where data and logs will be stored
* Generate a new root API key for your server, and set up config files based
  upon this token
* Restore the dumps of your account's API keys to your MongoDB instance
* Configure Zookeeper and Kafka
* Update platform config files

## Running

The Precog platform service actually consists of six interdependent services
that handle authentication, account management, ingest, querying, and job
management. Any service may be restarted at any time, usually without affecting
the other services, though in some cases other services will operate in a
degraded mode during the restart; this is particularly true of the
authentication service, although if authentication information is cached at the
time of the restart then there will usually be no problem. To run all of the
services, simply run the `run.sh` script.

    ./run.sh

Once the services are running, you will be able to connect to each of the services
independently; for example, you can test the ingest service by running the following:

    curl -X POST http://localhost:63770/ingest/v1/fs/<your-10-digit-accountId>/test?apiKey=<your-api-key>&mode=batch -H "Content-Type: application/json" -d '{"testField": "testValue"}'

 To query this data, try:

    curl "http://localhost:63774/analytics/v1/fs/<your-10-digit-accountId>?apiKey=<your-api-key>&q=count\(//test\)"

 Access to the remaining services follows the same pattern. In our production
 environment, we use haproxy to provide proxying, load balancing, and failover;
 it is possible to run multiple ingest nodes, for example, to provide high
 availability.
