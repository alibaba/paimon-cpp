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

.. _building-docs:

Building the Documentation
==========================

Prerequisites
-------------

The documentation build relies on `Doxygen <http://www.doxygen.nl/>`_ and
`Sphinx <http://www.sphinx-doc.org/>`_ along with a few extensions.

First, install `Doxygen <http://www.doxygen.nl/>`_
yourself (for example from your distribution's official repositories, if
using Linux).  Then install the Python-based requirements with the
following command:

.. code-block:: shell

   pip install -r paimon-cpp/docs/requirements.txt

Building
--------

.. note::

   If you are building the documentation on Windows, not all sections
   may build properly.

These two steps are mandatory and must be executed in order.

#. Process the C++ API using Doxygen

   .. code-block:: shell

      cd paimon-cpp/apidoc
      doxygen
      cd -

#. Build the complete documentation using Sphinx.

   .. code-block:: shell

      cd paimon-cpp/docs
      make html
      cd -


After these steps are completed, the documentation is rendered in HTML
format in ``paimon-cpp/docs/_build/html``.  In particular, you can point your
browser at ``paimon-cpp/docs/_build/html/index.html`` to read the docs and
review any changes you made.
