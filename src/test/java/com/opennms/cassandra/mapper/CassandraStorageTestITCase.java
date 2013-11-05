package com.opennms.cassandra.mapper;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


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
        u.setAddresses(Lists.newArrayList(addresses));

        m_sampleAddresses = addresses;
        m_sampleUser = u;

        m_storage = new CassandraStorage("localhost", 9042, "mapper_test");

    }

    @Test
    public void testCreate() {

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
    public void testUpdate() {

        User user = new User("Eric", "Evans", "eevans@opennms.com");
        user.setId(m_storage.create(user));

        user.setEmail("eevans@opennms.org");

        m_storage.update(user);

        User read = get(m_storage.read(User.class, user.getId()));

        assertEquals(user.getEmail(), read.getEmail());

    }

    @Test
    public void testUpdateNoDiff() {

        User user = new User("Eric", "Evans", "eevans@opennms.com");
        user.setId(m_storage.create(user));

        m_storage.update(user);

        User read = get(m_storage.read(User.class, user.getId()));

        assertEquals(user.getEmail(), read.getEmail());
        assertEquals(user.getSurname(), read.getSurname());
        assertEquals(user.getEmail(), read.getEmail());

    }

    @Test
    public void testDelete() {

        // Write, read, verify presence.
        UUID id = m_storage.create(new User("Eric", "Evans", "eevans@opennms.com"));
        Optional<User> read = m_storage.read(User.class, id);

        assertTrue(read.isPresent());

        // Delete, verify absence.
        m_storage.delete(read.get());

        assertFalse(m_storage.read(User.class, id).isPresent());

    }

    @Test
    public void testCreateOneToMany() {

        persistSampleUser();

        User read = get(m_storage.read(User.class, m_sampleUser.getId()));

        Set<Address> past = Sets.newHashSet(m_sampleUser.getAddresses());
        Set<Address> present = Sets.newHashSet(read.getAddresses());
        assertEquals(past, present);

    }

    @Test
    public void testUpdateWithOneToMany() {

        persistSampleUser();

        // Persist an additional address, re-read, and ensure present.
        Address added = new Address("Sunset Blvd", "Los Angeles", "90069");
        added.setId(m_storage.create(added));

        m_sampleUser.getAddresses().add(added);
        m_storage.update(m_sampleUser);

        User read = get(m_storage.read(User.class, m_sampleUser.getId()));

        assertTrue(read.getAddresses().contains(added));

        // Remove one address, re-read, and ensure absent.
        Address removed = m_sampleAddresses[0];
        read.getAddresses().remove(removed);
        m_storage.update(read);

        read = get(m_storage.read(User.class, m_sampleUser.getId()));

        assertFalse(read.getAddresses().contains(removed));

    }

    @Test(expected = IllegalStateException.class)
    public void testUpdateWithUnpersistedRelations() {

        persistSampleUser();

        // Update with an unpersisted address (should except).
        m_sampleUser.getAddresses().add(new Address("Sunset Blvd", "Los Angeles", "90069"));

        m_storage.update(m_sampleUser);

    }

    @Test
    public void testWithIndexedColumn() {

        persistSampleUser();

        User read = get(m_storage.read(User.class, "email", m_sampleUser.getEmail()));

        assertEquals(m_sampleUser.getId(), read.getId());
        assertEquals(m_sampleUser.getEmail(), read.getEmail());

    }
    
    @Test
    public void testUpdateIndexedColumn() {

        persistSampleUser();

        m_sampleUser.setEmail("root@matrix.com");
        m_storage.update(m_sampleUser);

        User read = get(m_storage.read(User.class, "email", m_sampleUser.getEmail()));

        assertEquals(m_sampleUser.getId(), read.getId());
        assertEquals(m_sampleUser.getEmail(), read.getEmail());

    }

    private void persistSampleUser() {
        for (Address a : m_sampleAddresses) a.setId(m_storage.create(a));
        m_sampleUser.setId(m_storage.create(m_sampleUser));
    }

    private <T> T get(Optional<T> optional) {
        assertTrue(optional.isPresent());
        return optional.get();
    }

}
