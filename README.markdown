mapper
======

 * You must have a no arg constructor.
 * Supported `javax.persistence` annotations: `Id`, `Column`, `OneToMany`
 * Additional annotations:

Limitations
-----------
 * There is (currently) only one type of index, `INVERTED`, and it maps a
   one-to-one relationship between the column value, and the ID of the
   object indexing it (`T read(Class<T>, String, Object)` assumes this as
   well).
 * Only field annotations are (currently) supported.