/*
 * Copyright 2013, The OpenNMS Group
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.opennms.lucidity;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.opennms.lucidity.CassandraEntityStoreFactory;
import com.opennms.lucidity.ConsistencyLevel;
import com.opennms.lucidity.EntityStore;


// FIXME: Ensure tests cover properties of all supported types.

public class CassandraStorageTestITCase {

    private EntityStore m_entityStore;
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
        u.getFavorites().put("meat", "beef");
        u.getFavorites().put("beverage", "beer");
        u.getFavorites().put("simpson", "bart");
        u.getDays().add("monday");
        u.getDays().add("tuesday");
        u.getDays().add("wednesday");
        u.getScores().add(1);
        u.getScores().add(2);
        u.getScores().add(4);
        u.getStatus().put("calm", "damaged");
        u.getStatus().put("patience", "limited");
        u.getSchedule().add("monday");
        u.getSchedule().add("wednesday");
        u.getSchedule().add("friday");

        m_sampleAddresses = addresses;
        m_sampleUser = u;
        m_entityStore = new CassandraEntityStoreFactory("localhost", 9042, "lucidity_test", ConsistencyLevel.ONE).createEntityStore();

    }

    @Test
    public void testCreate() {

        // Persist user and then read it back by ID.
        persistSampleUser();
        User user = get(m_entityStore.read(User.class, m_sampleUser.getId()));

        assertEquals(m_sampleUser.getGiven(), user.getGiven());
        assertEquals(m_sampleUser.getSurname(), user.getSurname());
        assertEquals(m_sampleUser.getEmail(), user.getEmail());
        assertEquals(m_sampleUser.getAge(), user.getAge());
        assertEquals(m_sampleUser.getLastUpdated(), user.getLastUpdated());
        assertEquals(m_sampleUser.getCreated(), user.getCreated());
        assertEquals(m_sampleUser.isValidated(), user.isValidated());
        assertEquals(m_sampleUser.getTemperature(), user.getTemperature(), 0.0d);

        assertEquals(m_sampleUser.getFavorites().size(), user.getFavorites().size());
        assertEquals(m_sampleUser.getFavorites().get("simpson"), user.getFavorites().get("simpson"));

        assertEquals(m_sampleUser.getDays().size(), user.getDays().size());
        assertTrue(user.getDays().contains("monday"));

        assertEquals(m_sampleUser.getScores().size(), user.getScores().size());
        assertEquals(m_sampleUser.getScores().get(2), user.getScores().get(2));

        assertEquals(m_sampleUser.getStatus().size(), user.getStatus().size());
        assertTrue(user.getStatus().containsKey("calm"));

    }

    @Test(expected = IllegalStateException.class)
    public void testCreateWithUnpersistedRelations() {
        // Save user without saving the address relations.
        m_entityStore.create(m_sampleUser);
        m_entityStore.read(User.class, m_sampleUser.getId());
    }

    @Test
    public void testUpdate() {

        User user = new User("Eric", "Evans", "eevans@opennms.com");
        User created = m_entityStore.create(user);

        user.setEmail("eevans@opennms.org");
        user.setAge(31);
        user.setLastUpdated(System.currentTimeMillis());
        user.setTemperature(99.9);
        user.setValidated(false);

        m_entityStore.update(created);

        User read = get(m_entityStore.read(User.class, user.getId()));

        assertEquals(user.getEmail(), read.getEmail());
        assertEquals(user.getAge(), read.getAge());
        assertEquals(user.getLastUpdated(), read.getLastUpdated());
        assertEquals(user.getTemperature(), read.getTemperature(), 0.0d);
        assertEquals(user.isValidated(), read.isValidated());

    }

    @Test
    public void testUpdateNoDiff() {

        User user = new User("Eric", "Evans", "eevans@opennms.com");
        User created = m_entityStore.create(user);

        m_entityStore.update(created);

        User read = get(m_entityStore.read(User.class, user.getId()));

        assertEquals(user.getEmail(), read.getEmail());
        assertEquals(user.getSurname(), read.getSurname());
        assertEquals(user.getEmail(), read.getEmail());

    }

    @Test
    public void testDelete() {

        // Write, read, verify presence.
        User user = new User("Eric", "Evans", "eevans@opennms.com");
        m_entityStore.create(user);
        User read = get(m_entityStore.read(User.class, user.getId()));

        // Delete, verify absence.
        m_entityStore.delete(read);

        assertFalse(m_entityStore.read(User.class, user.getId()).isPresent());

    }

    @Test
    public void testCreateOneToMany() {

        persistSampleUser();

        User read = get(m_entityStore.read(User.class, m_sampleUser.getId()));

        Set<Address> past = Sets.newHashSet(m_sampleUser.getAddresses());
        Set<Address> present = Sets.newHashSet(read.getAddresses());
        assertEquals(past, present);

    }

    @Test
    public void testUpdateWithOneToMany() {

        User user = persistSampleUser();

        // Persist an additional address, re-read, and ensure present.
        Address added = new Address("Sunset Blvd", "Los Angeles", "90069");
        m_entityStore.create(added);

        m_sampleUser.getAddresses().add(added);
        m_entityStore.update(user);

        User read = get(m_entityStore.read(User.class, m_sampleUser.getId()));

        assertTrue(read.getAddresses().contains(added));

        // Remove one address, re-read, and ensure absent.
        Address removed = m_sampleAddresses[0];
        read.getAddresses().remove(removed);
        m_entityStore.update(read);

        read = get(m_entityStore.read(User.class, m_sampleUser.getId()));

        assertFalse(read.getAddresses().contains(removed));

    }

    @Test(expected = IllegalStateException.class)
    public void testUpdateWithUnpersistedRelations() {

        User user = persistSampleUser();

        // Update with an unpersisted address (should except).
        m_sampleUser.getAddresses().add(new Address("Sunset Blvd", "Los Angeles", "90069"));

        m_entityStore.update(user);

    }

    @Test(expected = IllegalStateException.class)
    public void testUpdateUntracked() {
        m_entityStore.update(new User("Peter", "Griffin", "pgriffin@fox.com"));
    }

    @Test
    public void testWithIndexedColumn() {

        persistSampleUser();

        User read = get(m_entityStore.read(User.class, "email", m_sampleUser.getEmail()));

        assertEquals(m_sampleUser.getId(), read.getId());
        assertEquals(m_sampleUser.getEmail(), read.getEmail());

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIndexReadWithoutIndexedColumn() {
        m_entityStore.read(Address.class, "city", "San Antonio");
    }

    @Test
    public void testUpdateIndexedColumn() {

        User user = persistSampleUser();

        m_sampleUser.setEmail("root@matrix.com");
        m_entityStore.update(user);

        User read = get(m_entityStore.read(User.class, "email", m_sampleUser.getEmail()));

        assertEquals(m_sampleUser.getId(), read.getId());
        assertEquals(m_sampleUser.getEmail(), read.getEmail());

    }

    @Test(expected = IllegalStateException.class)
    public void testCreateOnClosedStore() throws IOException {
        m_entityStore.close();
        persistSampleUser();
    }

    @Test(expected = IllegalStateException.class)
    public void testReadOnClosedStore() throws IOException {
        m_entityStore.close();
        m_entityStore.read(User.class, UUID.randomUUID());
    }

    @Test(expected = IllegalStateException.class)
    public void testReadIndexedOnClosedStore() throws IOException {
        m_entityStore.close();
        m_entityStore.read(User.class, "email", "eevans@opennms.org");
    }

    @Test(expected = IllegalStateException.class)
    public void testUpdateOnClosedStore() throws IOException {
        m_entityStore.close();
        m_entityStore.update(persistSampleUser());
    }

    @Test(expected = IllegalStateException.class)
    public void testDeleteOnClosedStore() throws IOException {
        m_entityStore.close();
        m_entityStore.delete(persistSampleUser());
    }

    @Test
    public void testUpdateMapColumnElementStrategy() {

        User inst0, inst1;

        inst0 = persistSampleUser();
        inst1 = get(m_entityStore.read(User.class, inst0.getId()));

        inst0.getFavorites().put("beverage", "ale");
        inst0.getFavorites().remove("simpson");
        inst1.getFavorites().put("metal", "progressive");

        m_entityStore.update(inst0);
        m_entityStore.update(inst1);

        User read = get(m_entityStore.read(User.class, inst0.getId()));

        assertTrue(read.getFavorites().containsKey("beverage"));
        assertEquals("ale", read.getFavorites().get("beverage"));
        assertTrue(!read.getFavorites().containsKey("simpson"));
        assertTrue(read.getFavorites().containsKey("metal"));

    }

    @Test
    public void testUpdateMapColumnAllStrategy() {

        User inst0, inst1;

        inst0 = persistSampleUser();
        inst1 = get(m_entityStore.read(User.class, inst0.getId()));

        inst0.getStatus().put("curiosity", "piqued");
        inst1.getStatus().put("mood", "mellowing");

        m_entityStore.update(inst0);
        m_entityStore.update(inst1);

        User read = get(m_entityStore.read(User.class, inst0.getId()));

        assertEquals(3, read.getStatus().size());
        assertFalse(read.getStatus().containsKey("curiosity"));
        assertTrue(read.getStatus().containsKey("mood"));
        assertEquals("mellowing", read.getStatus().get("mood"));

    }

    @Test
    public void testUpdateSetColumnElementStrategy() {

        User inst0, inst1;

        inst0 = persistSampleUser();
        inst1 = get(m_entityStore.read(User.class, inst0.getId()));

        inst0.getDays().add("friday");
        inst0.getDays().remove("tuesday");
        inst1.getDays().add("saturday");

        m_entityStore.update(inst0);
        m_entityStore.update(inst1);

        User read = get(m_entityStore.read(User.class, inst0.getId()));

        assertEquals(4, read.getDays().size());
        assertTrue(read.getDays().contains("monday"));
        assertTrue(read.getDays().contains("wednesday"));
        assertTrue(read.getDays().contains("friday"));
        assertTrue(read.getDays().contains("saturday"));

    }

    @Test
    public void testUpdateSetColumnAllStrategy() {

        User inst0, inst1;

        inst0 = persistSampleUser();
        inst1 = get(m_entityStore.read(User.class, inst0.getId()));

        inst0.getSchedule().remove("monday");
        inst0.getSchedule().add("tuesday");
        inst1.getSchedule().add("saturday");
        inst1.getSchedule().add("sunday");
        inst1.getSchedule().remove("wednesday");

        m_entityStore.update(inst0);
        m_entityStore.update(inst1);

        User read = get(m_entityStore.read(User.class, inst0.getId()));

        assertEquals(4, read.getSchedule().size());
        assertTrue(read.getSchedule().contains("sunday"));
        assertTrue(read.getSchedule().contains("monday"));
        assertTrue(read.getSchedule().contains("friday"));
        assertTrue(read.getSchedule().contains("saturday"));

    }

    @Test
    public void testUpdateListColumn() { // COLLECTION update strategy

        User sample = persistSampleUser();

        sample.setScores(Arrays.asList(new Integer[] { 8, 16, 32 }));

        m_entityStore.update(sample);

        User read = get(m_entityStore.read(User.class, sample.getId()));

        assertEquals(3, read.getScores().size());
        assertEquals(Integer.valueOf(8), read.getScores().get(0));
        assertEquals(Integer.valueOf(16), read.getScores().get(1));
        assertEquals(Integer.valueOf(32), read.getScores().get(2));

    }

    private User persistSampleUser() {
        for (Address a : m_sampleAddresses) m_entityStore.create(a);
        return m_entityStore.create(m_sampleUser);
    }

    private <T> T get(Optional<T> ref) {
        assertNotNull(ref.get());
        return ref.get();
    }

}
