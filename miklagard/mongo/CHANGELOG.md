Precog for MongoDB Changelog
============================

Version 1.3.0
-------------
* Numerous bug fixes and performance improvements
* A small change to the configuration file format:

The lines

        query {
          v1 {

need to change to

        analytics {
          v2 {


Version 1.2.0
-------------
* Add the "include_ids" configuration parameter to include Mongo's
"\_id" field to results. To enable, add the parameter to the queryExecutor block. For example:

        queryExecutor {
          include_ids = true
          mongo {
            server = "mongodb://localhost:27017"
          }
        }

Version 1.1.2
-------------
* Fix a bug resulting in concurrent usage of Mongo cursors
* Miscellaneous evaluator bugfixes
	
Version 1.1.1
-------------

* Fix a bug in table loads that caused join queries to return
empty sets

Version 1.1
-----------
* Add support for database authentication for MongoDB (see
README.md for configuration details)

* Improve performance of field filtering. For example, running a
`count(load("/foo/bar").test)` will generally perform faster than
`count(//foo/bar)`, since mongo will only load the `test` property
instead of whole documents.

Version 1.0.4
-------------
* Fix a bug in the quirrel solver that could cause hangs or
invalid results
