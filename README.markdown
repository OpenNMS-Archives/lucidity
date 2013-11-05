Cassandra Object Mapper
=======================

Synopsis
--------

    // Creating
    Storage storage = new CassandraStorage("localhost", 9042, "keyspace");
    User user = new User(username, firstName, lastName);
    
    storage.create(user);
    
    // Reading
    User user = storage.read(User.class, id);
    


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