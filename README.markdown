Lucidity
========

Lucidity is a Java object-mapper for Cassandra.  


Usage
-----

    // User.java
    import javax.persistence.*;
    
    @Entity
    @Table(name="users")
    public class User {
        @Id
        @Column(name="id")
        private UUID id;
        
        @Column(name="given")
        private String givenName;
        
        @Column(name="surname")
        private String surname;
        
        ...
        
        // Getters, setters, etc
    }

Objects persisted with Lucidity must:

  * Have a nullary (no-arg) constructor.
  * Be annotated with `@Entity`.
  * Have exactly one property annotated with `@Id`, the type must be
    `java.util.UUID`.
  * Have at least one `@Column` annotated property.

The `@Table` annotation is optional, if ommited the name of the class is
used as the table name.  The `name` argument to `@Column` is also optional,
and will default to the name of the property if unused.

_Note: Currently only field annoations are supported._


    EntityStore storage = new CassandraEntityStore("localhost", 9042, "keyspace");
    
    // Creating
    User user = new User(givenName, lastName);
    Session<User> session = storage.create(user, ConsistencyLevel.QUORUM);
    
    // Updating
    user.setSurname(name);
    storage.update(session);
    
    // Reading
    Session<User> session = storage.read(User.class, id);
    User user = session.get();
    
`Session`s are returned by operations that read, or create entities and are
used to track changes when making updates.  In the example above, the call
to `EntityStore#update(Session)` would result in an `UPDATE` of only the
`users.surname` column.

One-to-many Relationships
-------------------------

    // Address.java
    @Entity
    @Table(name="addresses")
    public class Address {
        @Id
        private UUID id;
        
        @Column
        private String city;
        
        @Column
        private String zipcode;

    // User.java
    @Entity
    @Table(name="users")
    public class User {
        ...
        
        @OneToMany
        private Collection<Address> addresses;
            
        ...
          
        // Getters, setters, etc
    }

A `@OneToMany` annotated field like the one above uses a join table in
Cassandra to map the IDs of the `Address` relations to the ID of the
`User` entity.  The DDL for the join table used above would look something
like the following:

    CREATE TABLE users_addresses (
            users_id uuid,
            addresses_id uuid,
            PRIMARY KEY (users_id, addresses_id)
    )

Persistence of an entity, and the relational meta-data in the join table is
atomic, (even without the batch log).

When an entity with one-to-many relationships is read, the related entities
are retrieved as well.

Indexing
--------

    // User.java
    @Entity
    @Table(name="users")
    public class User {
        ...
        
        @Index(type=INVERTED)
        @Column(name="email")
        private String emailAddress;
            
        ...
          
        // Getters, setters, etc
    }


    
Limitations
-----------
 * There is (currently) only one type of index, `INVERTED`, and it maps a
   one-to-one relationship between the column value, and the ID of the
   object indexing it (`T read(Class<T>, String, Object)` assumes this as
   well).
 * Only field annotations are (currently) supported.