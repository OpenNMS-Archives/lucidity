package com.opennms.cassandra.mapper;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Date;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;


// FIXME: Ensure tests cover properties of all supported types.

public class CassandraStorageTestITCase {

    private Storage m_storage;

    @Before
    public void setUp() throws Exception {
        m_storage = new CassandraStorage("localhost", 9042, "mapper_test");
    }

    @Test
    public void testCreate() throws StorageException {

        User user = new User("Eric", "Evans", "eevans@opennms.com");
        user.setAge(25);
        user.setLastUpdated(System.currentTimeMillis());
        user.setCreated(new Date());
        user.setTemperature(98.6);
        user.setValidated(true);

        UUID id = m_storage.create(user);

        User read = m_storage.read(User.class, id);

        assertEquals(user.getGiven(), read.getGiven());
        assertEquals(user.getSurname(), read.getSurname());
        assertEquals(user.getEmail(), read.getEmail());
        assertEquals(user.getAge(), read.getAge());
        assertEquals(user.getLastUpdated(), read.getLastUpdated());
        assertEquals(user.getCreated(), read.getCreated());
        assertEquals(user.isValidated(), read.isValidated());

    }

    @Test
    public void testUpdate() throws StorageException {

        User user = new User("Eric", "Evans", "eevans@opennms.com");
        UUID id = m_storage.create(user);

        user.setEmail("eevans@opennms.org");
        user.setId(id);

        m_storage.update(user);

        User read = m_storage.read(User.class, id);

        assertEquals(user.getEmail(), read.getEmail());

    }

    @Test
    public void testUpdateNoDiff() throws StorageException {

        User user = new User("Eric", "Evans", "eevans@opennms.com");
        UUID id = m_storage.create(user);

        user.setId(id);

        m_storage.update(user);

        User read = m_storage.read(User.class, id);

        assertEquals(user.getEmail(), read.getEmail());
        assertEquals(user.getSurname(), read.getSurname());
        assertEquals(user.getEmail(), read.getEmail());

    }

    @Test(expected = StorageException.class)
    public void testDelete() throws StorageException {

        UUID id = m_storage.create(new User("Eric", "Evans", "eevans@opennms.com"));
        User read = m_storage.read(User.class, id);
        assertNotNull(read);

        m_storage.delete(read);

        read = m_storage.read(User.class, id);

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

        User read = m_storage.read(User.class, userID);
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

        User read = m_storage.read(User.class, "email", "eevans@opennms.com");

        assertEquals(id, read.getId());

    }
    
}
