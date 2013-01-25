Precog for PostgreSQL ChangeLog
===============================

Version 1.1.0
-------------
* Added support for hstore and json columns
* Numerous bug fixes

*Note:* While json and hstore columns are supported for read, PostgreSQL doesn't currently support key/property level queries against them. Because of that, any queries that utilize hstore or json columns will read all contained column data before filtering down to only the required columns.

Version 1.0.0
-------------
* Initial release
