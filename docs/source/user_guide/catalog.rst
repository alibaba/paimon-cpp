.. Copyright 2024-present Alibaba Inc.

.. Licensed under the Apache License, Version 2.0 (the "License");
.. you may not use this file except in compliance with the License.
.. You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

.. _catalog:

Catalog
==========================
C++ Paimon provides a :ref:`Catalog abstraction <cpp-api-catalog>` to manage the table of contents and metadata. The Catalog
abstraction provides a series of ways to help you better integrate with computing engines. We always
recommend that you use Catalog to access the Paimon table.

C++ Paimon supports two metastores, selected with the catalog option
``metastore``: the filesystem metastore (default) and the REST metastore.

Filesystem Catalog
~~~~~~~~~~~~~~~~~~
The filesystem metastore (``metastore=filesystem``, the default) stores both
metadata and table files in filesystems. The ``root_path`` argument of
``Catalog::Create`` is the warehouse directory holding the databases and tables.

REST Catalog
~~~~~~~~~~~~
With the REST metastore (``metastore=rest``), catalog metadata is managed by a
remote catalog server exposed through a REST API; table data itself is still read
and written through the table paths returned by the server. See `Java Paimon REST
Catalog <https://paimon.apache.org/docs/master/concepts/rest/>`_ for the concept
and the server-side protocol.

REST catalog support is an optional build component: configure the build with
``-DPAIMON_ENABLE_REST=ON`` (see :ref:`cpp_build_optional_components`).

When ``metastore=rest``, the ``root_path`` argument of ``Catalog::Create`` is not
a filesystem path but the warehouse (instance) name under which the tables are
registered on the REST server. The catalog is configured through the
``CatalogOptions`` keys:

* ``metastore``: must be ``rest`` to select the REST catalog.
* ``uri``: server url of the REST catalog server.
* ``token.provider``: authentication provider of the REST catalog; currently only
  ``bear`` is supported (the protocol's historical spelling of "bearer").
* ``token``: token of the ``bear`` token provider.
* ``table-default.<key>``: table option defaults applied when a created table
  left ``<key>`` unset.
* ``header.<name>``: sent as the ``<name>`` http header on every request to the
  server. The server may configure headers of its own through the ``/v1/config``
  endpoint, which are merged with these as any other option is.

.. code-block:: cpp

   std::map<std::string, std::string> options = {
       {"metastore", "rest"},
       {"uri", "http://127.0.0.1:8080"},
       {"token.provider", "bear"},
       {"token", "<token>"},
   };
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::Catalog> catalog,
                          paimon::Catalog::Create(/*root_path=*/"my_instance", options));

On creation the catalog queries the server's ``/v1/config`` endpoint and merges
its response with the options above: the server's overrides win over the client
options, which in turn win over the server's defaults.

Databases and tables are then created, listed, loaded, renamed and dropped
through the regular ``Catalog`` API, and table snapshots can be listed through
``Catalog::ListSnapshots``.

The C++ REST catalog covers the database, table and snapshot operations of the
``Catalog`` API. The parts of the Java REST catalog that have no C++ counterpart
yet — altering a database or a table, views, functions, partitions, tags, branch
management and consumers — are not supported, and neither is the ``dlf`` token
provider.
