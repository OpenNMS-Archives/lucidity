package com.opennms.cassandra.mapper;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;


// FIXME: Ensure tests cover properties of all supported types.

public class CassandraStorageTestITCase {

    private Storage m_storage;
    private User m_sampleUser;
    private Address[] m_sampleAddresses;

    @Before
    public void setUp() throws Exception {

        Address[] addresses = new Address[] { 
                new Address("Dove Flight", "San Antonio", "78250"),
                new Address("Pecan Stree", "San Antonio", "78205")
        };

        User u = new User("Thomas", "Anderson", "neo@whiterabbit.org");
        u.setAge(30);
        u.setCreated(new Date());
        u.setLastUpdated(System.currentTimeMillis());
        u.setTemperature(98.6);
        u.setValidated(true);
        u.setAddresses(Arrays.asList(addresses));

        m_sampleAddresses = addresses;
        m_sampleUser = u;

        m_storage = new CassandraStorage("localhost", 9042, "mapper_test");

    }

    @Test
    public void testCreate() throws StorageException {

        // Persist associated addresses
        for (Address a : m_sampleAddresses) {
            a.setId(m_storage.create(a));
        }

        // Persist user and then read it back by ID.
        User user = get(m_storage.read(User.class, m_storage.create(m_sampleUser)));

        assertEquals(m_sampleUser.getGiven(), user.getGiven());
        assertEquals(m_sampleUser.getSurname(), user.getSurname());
        assertEquals(m_sampleUser.getEmail(), user.getEmail());
        assertEquals(m_sampleUser.getAge(), user.getAge());
        assertEquals(m_sampleUser.getLastUpdated(), user.getLastUpdated());
        assertEquals(m_sampleUser.getCreated(), user.getCreated());
        assertEquals(m_sampleUser.isValidated(), user.isValidated());
        assertEquals(m_sampleUser.getTemperature(), user.getTemperature(), 0.0d);

    }

    @Test(expected = IllegalStateException.class)
    public void testCreateWithUnpersistedRelations() throws StorageException {
        // Save user without saving the address relations.
        m_storage.read(User.class, m_storage.create(m_sampleUser));
    }

    @Test
    public void testUpdate() throws StorageException {

        User user = new User("Eric", "Evans", "eevans@opennms.com");
        user.setId(m_storage.create(user));

        user.setEmail("eevans@opennms.org");

        m_storage.update(user);

        User read = get(m_storage.read(User.class, user.getId()));

        assertEquals(user.getEmail(), read.getEmail());

    }

    @Test
    public void testUpdateNoDiff() throws StorageException {

        User user = new User("Eric", "Evans", "eevans@opennms.com");
        UUID id = m_storage.create(user);

        user.setId(id);

        m_storage.update(user);

        User read = get(m_storage.read(User.class, id));

        assertEquals(user.getEmail(), read.getEmail());
        assertEquals(user.getSurname(), read.getSurname());
        assertEquals(user.getEmail(), read.getEmail());

    }

    @Test(expected = StorageException.class)
    public void testDelete() throws StorageException {

        UUID id = m_storage.create(new User("Eric", "Evans", "eevans@opennms.com"));
        User read = get(m_storage.read(User.class, id));
        assertNotNull(read);

        m_storage.delete(read);

        read = get(m_storage.read(User.class, id));

    }

    @Test
    public void testCreateWithOneToMany() throws StorageException {

        Address address0 = new Address("Dove Flight", "San Antonio", "78250");
        Address address1 = new Address("Pecan Street", "San Antonio", "78205");
        address0.setId(m_storage.create(address0));
        address1.setId(m_storage.create(address1));

        User user = new User("Eric", "Evans", "eevans@opennms.com");
        user.setAddresses(Lists.newArrayList(address0, address1));

        UUID userID = m_storage.create(user);

        System.err.println(m_storage.read(User.class, userID));

    }

    @Test
    public void testUpdateWithOneToMany() throws StorageException {

        Address address0 = new Address("Dove Flight", "San Antonio", "78250");
        Address address1 = new Address("Pecan Street", "San Antonio", "78205");
        address0.setId(m_storage.create(address0));
        address1.setId(m_storage.create(address1));

        User user = new User("Eric", "Evans", "eevans@opennms.com");
        user.setAddresses(Lists.newArrayList(address0, address1));

        UUID userID = m_storage.create(user);

        User read = get(m_storage.read(User.class, userID));
        read.getAddresses().remove(read.getAddresses().iterator().next());
        m_storage.update(read);

        System.err.println(m_storage.read(User.class, userID));

    }

    @Test
    public void testWithIndexedColumn() throws StorageException {

        User user = new User("Eric", "Evans", "eevans@opennms.com");
        user.setAge(25);
        user.setLastUpdated(System.currentTimeMillis());
        user.setCreated(new Date());
        user.setTemperature(98.6);
        user.setValidated(true);

        UUID id = m_storage.create(user);

        User read = get(m_storage.read(User.class, "email", "eevans@opennms.com"));

        assertEquals(id, read.getId());

    }

    private <T> T get(Optional<T> optional) {
        assertTrue(optional.isPresent());
        return optional.get();
    }

}
