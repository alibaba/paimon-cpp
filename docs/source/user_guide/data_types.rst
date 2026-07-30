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

Data Types
==========

A data type describes the logical type of a value in the table ecosystem. It can
be used to declare input and/or output types of operations.

All data types by Java Paimon are as follows, C++ Paimon uses Apache Arrow as
its schema representation. The following table shows the mapping between `Java
Paimon DataTypes <https://paimon.apache.org/docs/master/concepts/data-types/>`_
and `Arrow DataTypes <https://arrow.apache.org/docs/format/Columnar.html#data-types>`_:

.. list-table::
   :header-rows: 1
   :widths: 10 10 30

   * - Java Paimon DataType
     - Arrow DataType
     - Description

   * - ``BOOLEAN``
     - Boolean
     - Data type of a boolean with a (possibly) three-valued logic of TRUE, FALSE, and UNKNOWN.

   * - ``CHAR``

       ``CHAR(n)``
     - Utf8
     - Data type of a fixed-length character string.

       The type can be declared using ``CHAR(n)`` where n is the number of code
       points. n must have a value between 1 and 2,147,483,647 (both inclusive).
       If no length is specified, n is equal to 1.

       **Note:** Apache Arrow has no fixed-length string type, so the declared
       length ``n`` is not enforced; values are mapped to a variable-length ``Utf8``.

   * - ``VARCHAR``

       ``VARCHAR(n)``

     - Utf8
     - Data type of a variable-length character string.

       The type can be declared using ``VARCHAR(n)`` where n is the maximum
       number of code points. n must have a value between 1 and 2,147,483,647
       (both inclusive). If no length is specified, n is equal to 1.

       **Note:** The declared maximum length ``n`` is not enforced.

   * - ``STRING``
     - Utf8
     - Data type of a variable-length character string. ``STRING`` is a synonym for ``VARCHAR(2147483647)``.

   * - ``BINARY``

       ``BINARY(n)``
     - Binary
     - Data type of a fixed-length binary string (=a sequence of bytes).

       The type can be declared using ``BINARY(n)`` where n is the number of
       bytes. n must have a value between 1 and 2,147,483,647 (both inclusive).
       If no length is specified, n is equal to 1.

       **Note:** Apache Arrow has no fixed-length binary type here, so the declared
       length ``n`` is not enforced; values are mapped to a variable-length ``Binary``.

   * - ``VARBINARY``

       ``VARBINARY(n)``
     - Binary
     - Data type of a variable-length binary string (=a sequence of bytes).

       The type can be declared using ``VARBINARY(n)`` where n is the maximum
       number of bytes. n must have a value between 1 and 2,147,483,647
       (both inclusive). If no length is specified, n is equal to 1.

       **Note:** The declared maximum length ``n`` is not enforced.

   * - ``BYTES``
     - Binary
     - ``BYTES`` is a synonym for ``VARBINARY(2147483647)``.

   * - ``DECIMAL``

       ``DECIMAL(p)``

       ``DECIMAL(p, s)``
     - Decimal128
     - Data type of a decimal number with fixed precision and scale.

       The type can be declared using ``DECIMAL(p, s)`` where p is the number of
       digits in a number (precision) and s is the number of digits to the right
       of the decimal point in a number (scale). p must have a value between 1
       and 38 (both inclusive). s must have a value between 0 and p
       (both inclusive). The default value for p is 10. The default value for s is 0.

   * - ``TINYINT``
     - Int8
     - Data type of a 1-byte signed integer with values from -128 to 127.

   * - ``SMALLINT``
     - Int16
     - Data type of a 2-byte signed integer with values from -32,768 to 32,767.

   * - ``INT``
     - Int32
     - Data type of a 4-byte signed integer with values from -2,147,483,648 to 2,147,483,647.

   * - ``BIGINT``
     - Int64
     - Data type of an 8-byte signed integer with values from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.

   * - ``FLOAT``
     - Float
     - Data type of a 4-byte single precision floating point number.

       Compared to the SQL standard, the type does not take parameters.

   * - ``DOUBLE``
     - Double
     - Data type of an 8-byte double precision floating point number.

   * - ``DATE``
     - Date32
     - Data type of a date consisting of year-month-day with values ranging from 0000-01-01 to 9999-12-31.

       Compared to the SQL standard, the range starts at year 0000.

   * - ``TIME``

       ``TIME(p)``
     - Not Supported
     - Data type of a time without time zone consisting of hour:minute:second[.fractional] with up to nanosecond precision and values ranging from 00:00:00.000000000 to 23:59:59.999999999.

       The type can be declared using ``TIME(p)`` where p is the number of digits of
       fractional seconds (precision). p must have a value between 0 and 9
       (both inclusive). If no precision is specified, p is equal to 0.

   * - ``TIMESTAMP``

       ``TIMESTAMP(p)``
     - Timestamp
     - Data type of a timestamp without time zone consisting of year-month-day hour:minute:second[.fractional] with up to nanosecond precision and values ranging from 0000-01-01 00:00:00.000000000 to 9999-12-31 23:59:59.999999999.

       The type can be declared using ``TIMESTAMP(p)`` where p is the number of
       digits of fractional seconds (precision). Paimon C++ supports precision
       values 0 (seconds), 3 (milliseconds), 6 (microseconds), and 9
       (nanoseconds). If no precision is specified, p is equal to 6.

   * - ``TIMESTAMP WITH LOCAL TIME ZONE``

       ``TIMESTAMP(p) WITH LOCAL TIME ZONE``
     - Timestamp
     - Data type of a timestamp with local time zone consisting of year-month-day hour:minute:second[.fractional] zone with up to nanosecond precision and values ranging from 0000-01-01 00:00:00.000000000 +14:59 to 9999-12-31 23:59:59.999999999 -14:59.

       This type fills the gap between time zone free and time zone mandatory
       timestamp types by allowing the interpretation of UTC timestamps according
       to the configured session time zone. A conversion from and to int describes
       the number of seconds since epoch. A conversion from and to long describes the number of milliseconds since epoch.

       Paimon C++ supports precision values 0 (seconds), 3 (milliseconds), 6
       (microseconds), and 9 (nanoseconds).

   * - ``ARRAY<t>``
     - List
     - Data type of an array of elements with same subtype.

       Compared to the SQL standard, the maximum cardinality of an array cannot be specified but is fixed at 2,147,483,647. Also, any valid type is supported as a subtype.

       The type can be declared using ``ARRAY<t>`` where t is the data type of the contained elements.

   * - ``MAP<kt, vt>``
     - Map
     - Data type of an associative array that maps keys to values (including NULL). A map cannot contain duplicate keys; each key can map to at most one value.

       There is no restriction of element types; it is the responsibility of the user to ensure uniqueness.

       The type can be declared using ``MAP<kt, vt>`` where kt is the data type of the key elements and vt is the data type of the value elements.

       **Note:** In C++ Paimon, map keys must be explicitly marked as ``NOT NULL``.
       Apache Arrow does not support nullable map keys. If the key type is not
       marked as ``NOT NULL`` in the schema, parsing will fail with an error.

   * - ``MULTISET<t>``
     - Not Supported
     - Data type of a multiset (=bag). Unlike a set, it allows for multiple instances for each of its elements with a common subtype. Each unique value (including NULL) is mapped to some multiplicity.

       There is no restriction of element types; it is the responsibility of the user to ensure uniqueness.

       The type can be declared using ``MULTISET<t>`` where t is the data type of the contained elements.

   * - ``ROW<n0 t0, n1 t1, ...>``

       ``ROW<n0 t0 'd0', n1 t1 'd1', ...>``
     - Struct
     - Data type of a sequence of fields.

       A field consists of a field name, field type, and an optional description.
       The most specific type of a row of a table is a row type. In this case,
       each column of the row corresponds to the field of the row type that has
       the same ordinal position as the column.

       Compared to the SQL standard, an optional field description simplifies
       the handling with complex structures.

       A row type is similar to the ``STRUCT`` type known from other non-standard-compliant frameworks.

       The type can be declared using ``ROW<n0 t0 'd0', n1 t1 'd1', ...>`` where n
       is the unique name of a field, t is the logical type of a field, d is the description of a field.

   * - ``VARIANT``
     - Struct
     - Data type of semi-structured data (e.g. JSON). A variant value contains one of:
       a primitive (e.g. integer, string), an array of variant values, or an object
       mapping string keys to variant values.

       Variant values are encoded with two binaries following the parquet-format
       `Variant Binary Encoding <https://github.com/apache/parquet-format/blob/master/VariantEncoding.md>`_
       specification (compatible with the Java Paimon / Spark implementation). In
       C++ Paimon, a variant field is represented in an Arrow schema as
       ``Struct{value: Binary NOT NULL, metadata: Binary NOT NULL}`` marked with
       Paimon-specific field metadata; use ``paimon::Variant::ArrowField`` to
       construct such a field, and ``paimon::Variant`` (``FromJson``/``ToJson``/
       ``VariantGet``) to build and inspect values.

       Only the parquet file format supports VARIANT columns. VARIANT cannot be
       used as a primary key, partition key or bucket key, and no predicate
       pushdown applies to it.

       When writing, variant columns can optionally be *shredded* into typed
       parquet columns per the parquet-format
       `Variant Shredding <https://github.com/apache/parquet-format/blob/master/VariantShredding.md>`_
       specification by setting ``variant.shreddingSchema`` to a ROW type JSON
       whose fields map top-level variant column names to their shredding
       types. Alternatively, setting ``variant.inferShreddingSchema`` to
       ``true`` infers a shredding schema per file from the first written rows
       (tuned by ``variant.shredding.maxSchemaWidth``, which bounds the total
       number of shredded fields across all variant columns of the schema,
       ``variant.shredding.maxSchemaDepth``,
       ``variant.shredding.minFieldCardinalityRatio`` and
       ``variant.shredding.maxInferBufferRow``). Inference also covers variant
       columns nested inside ROW columns (variants inside arrays or maps stay
       unshredded, as in Java Paimon). Readers reassemble shredded columns
       transparently.

       When reading, instead of the full variant, specific paths can be
       extracted by replacing the variant column in the read schema with a
       projection built by ``paimon::VariantAccessBuilder`` (e.g. ``$.a.b`` as
       BIGINT). For shredded files only the required typed sub-columns are
       read.
