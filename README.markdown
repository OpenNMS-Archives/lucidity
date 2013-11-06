Cassandra Object Mapper
=======================

Example
-------

    // User.java
    import javax.persistence.*;
    
    @Entity
    public class User {
        @Id
        @Column(name="userid")
        private UUID id;
        
        @Column(name="given_name")
        private String givenName;
        
        @Column(name="surname")
        private String surname;
        
        ...
    }

    Storage storage = new CassandraStorage("localhost", 9042, "keyspace");
    
    // Creating
    User user = new User(givenName, lastName);
    Session<User> session = storage.create(user);
    
    // Updating
    user.setGivenName(name);
    storage.update(session);
    
    // Reading
    Session<User> session = storage.read(User.class, id);
    User user = session.get();
    

Limitations
-----------
 * There is (currently) only one type of index, `INVERTED`, and it maps a
   one-to-one relationship between the column value, and the ID of the
   object indexing it (`T read(Class<T>, String, Object)` assumes this as
   well).
 * Only field annotations are (currently) supported.