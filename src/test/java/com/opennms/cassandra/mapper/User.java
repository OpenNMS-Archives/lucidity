package com.opennms.cassandra.mapper;


import static com.opennms.cassandra.mapper.IndexType.INVERTED;

import java.util.Collection;
import java.util.Date;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;


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

    @Override
    public String toString() {
        return String.format("%s[addresses=%s]", getClass().getSimpleName(), getAddresses());
    }
    
}
