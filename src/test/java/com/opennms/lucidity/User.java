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


import static com.opennms.lucidity.annotations.IndexType.INVERTED;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.opennms.lucidity.annotations.Column;
import com.opennms.lucidity.annotations.EmbeddedCollection;
import com.opennms.lucidity.annotations.Entity;
import com.opennms.lucidity.annotations.Id;
import com.opennms.lucidity.annotations.Index;
import com.opennms.lucidity.annotations.OneToMany;
import com.opennms.lucidity.annotations.Table;
import com.opennms.lucidity.annotations.UpdateStrategy;


@Entity
@Table(name = "users")
class User {

    @Id
    @Column(name = "id")
    private UUID m_id;

    @Column(name = "given")
    private String m_given;

    @Column(name = "surname")
    private String m_surname;

    @Index(type = INVERTED)
    @Column(name = "email")
    private String m_email;

    @Column(name = "last_updated")
    private long m_lastUpdated;

    @Column(name = "age")
    private int m_age;

    @Column(name = "created")
    private Date m_created;

    @Column(name = "validated")
    private boolean m_validated;

    @Column(name = "temperature")
    private double m_temperature;

    @EmbeddedCollection
    @Column(name = "favorites")
    private Map<String, String> m_favorites = Maps.newHashMap();

    @EmbeddedCollection(updateStrategy = UpdateStrategy.COLLECTION)
    @Column(name = "status")
    private Map<String, String> m_status = Maps.newHashMap();

    @EmbeddedCollection
    @Column(name = "days")
    private Set<String> m_days = Sets.newHashSet();

    @EmbeddedCollection(updateStrategy = UpdateStrategy.COLLECTION)
    @Column(name = "schedule")
    private Set<String> m_schedule = Sets.newHashSet();

    @EmbeddedCollection(updateStrategy = UpdateStrategy.COLLECTION)
    @Column(name = "scores")
    private List<Integer> m_scores = Lists.newArrayList();
    
    @OneToMany
    private Collection<Address> m_addresses;

    User() {

    }

    User(String given, String surname, String email) {
        setGiven(given);
        setSurname(surname);
        setEmail(email);
    }

    UUID getId() {
        return m_id;
    }

    void setId(UUID id) {
        m_id = id;
    }

    String getGiven() {
        return m_given;
    }

    void setGiven(String givenName) {
        m_given = givenName;
    }

    String getSurname() {
        return m_surname;
    }

    void setSurname(String surname) {
        m_surname = surname;
    }

    String getEmail() {
        return m_email;
    }

    void setEmail(String email) {
        m_email = email;
    }

    long getLastUpdated() {
        return m_lastUpdated;
    }

    void setLastUpdated(long lastUpdated) {
        m_lastUpdated = lastUpdated;
    }

    int getAge() {
        return m_age;
    }

    void setAge(int age) {
        m_age = age;
    }

    Date getCreated() {
        return m_created;
    }

    void setCreated(Date created) {
        m_created = created;
    }

    boolean isValidated() {
        return m_validated;
    }

    void setValidated(boolean validated) {
        m_validated = validated;
    }

    double getTemperature() {
        return m_temperature;
    }

    void setTemperature(double temperature) {
        m_temperature = temperature;
    }

    Collection<Address> getAddresses() {
        return m_addresses;
    }

    void setAddresses(Collection<Address> addresses) {
        m_addresses = addresses;
    }

    Map<String, String> getFavorites() {
        return m_favorites;
    }

    void setFavorites(Map<String, String> favorites) {
        m_favorites = favorites;
    }

    Set<String> getDays() {
        return m_days;
    }

    void setDays(Set<String> days) {
        m_days = days;
    }

    List<Integer> getScores() {
        return m_scores;
    }

    void setScores(List<Integer> scores) {
        m_scores = scores;
    }

    Map<String, String> getStatus() {
        return m_status;
    }

    void setStatus(Map<String, String> status) {
        m_status = status;
    }

    Set<String> getSchedule() {
        return m_schedule;
    }

    void setSchedule(Set<String> schedule) {
        m_schedule = schedule;
    }

    @Override
    public String toString() {
        return String.format("%s[addresses=%s]", getClass().getSimpleName(), getAddresses());
    }

}
