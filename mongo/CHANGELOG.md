Precog for MongoDB Changelog
==========

Version 1.1
----------
* Add support for database authentication for MongoDB (see
README.md for configuration details)

* Improve performance of field filtering. For example, running a
`count(load("/foo/bar").test)` will generally perform faster than
`count(//foo/bar)`, since mongo will only load the `test` property
instead of whole documents.

Version 1.0.4
----------
* Fix a bug in the quirrel solver that could cause hangs or
invalid results
