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

Filesystem Catalog
~~~~~~~~~~~~~~~~~~
C++ Paimon catalog currently support one types of metastores filesystem metastore (default),
which stores both metadata and table files in filesystems.

.. note::

  Current C++ Paimon only supports filesystem catalog. In the future, we will
  support REST catalog.
  By using the Paimon REST catalog, changes to the catalog will be directly stored
  in a remote catalog server which exposed through REST API. See `Java Paimon REST
  Catalog <https://paimon.apache.org/docs/master/concepts/rest/overview/>`_.
