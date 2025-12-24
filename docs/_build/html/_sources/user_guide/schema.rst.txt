.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. Borrowed the file from Apache Paimon:
.. https://github.com/apache/paimon/blob/master/docs/content/concepts/spec/schema.md

Schema
======

The version of the schema file starts from 0 and currently retains all versions of the schema.
There may be old files that rely on the old schema version, so its deletion should be done with caution.

Schema File is JSON, it includes:

1. ``fields``: data field list. Each data field contains ``id``, ``name``, ``type``. Field ``id`` is used to support schema evolution.
2. ``partitionKeys``: field name list. Partition definition of the table; it cannot be modified.
3. ``primaryKeys``: field name list. Primary key definition of the table; it cannot be modified.
4. ``options``: ``map<string, string>``, unordered. Options of the table, including a lot of capabilities and optimizations.

Example
-------

.. code-block:: json

   {
     "version" : 3,
     "id" : 0,
     "fields" : [ {
       "id" : 0,
       "name" : "order_id",
       "type" : "BIGINT NOT NULL"
     }, {
       "id" : 1,
       "name" : "order_name",
       "type" : "STRING"
     }, {
       "id" : 2,
       "name" : "order_user_id",
       "type" : "BIGINT"
     }, {
       "id" : 3,
       "name" : "order_shop_id",
       "type" : "BIGINT"
     } ],
     "highestFieldId" : 3,
     "partitionKeys" : [ ],
     "primaryKeys" : [ "order_id" ],
     "options" : {
       "bucket" : "5"
     },
     "comment" : "",
     "timeMillis" : 1720496663041
   }

Compatibility
-------------

For old versions:

- Version 1: should put ``bucket -> 1`` to ``options`` if there is no ``bucket`` key.
- Versions 1 & 2: should put ``file.format -> orc`` to ``options`` if there is no ``file.format`` key.

DataField
---------

DataField represents a column of the table.

1. ``id``: int, column id, automatic increment; it is used for schema evolution.
2. ``name``: string, column name.
3. ``type``: data type, very similar to SQL type string.
4. ``description``: string.

Update Schema
-------------

Updating the schema should generate a new schema file.

.. code-block:: text

   warehouse
   └── default.db
       └── my_table
           ├── schema
               ├── schema-0
               ├── schema-1
               └── schema-2

There is a reference to schema in the snapshot. The schema file with the highest numerical value is usually the latest schema file.

Old schema files cannot be directly deleted because there may be old data files that reference old schema files. When
reading the table, it is necessary to rely on them for schema evolution reading.
