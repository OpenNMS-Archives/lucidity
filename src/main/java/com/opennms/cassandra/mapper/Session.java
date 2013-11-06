package com.opennms.cassandra.mapper;


import java.util.UUID;


/**
 * Represents a session of a persisted object.
 *
 * @author eevans
 * @param <T>
 *            persisted type
 */
public class Session<T> {

    private final UUID m_id = UUID.randomUUID();
    private final T m_obj;

    public Session(T obj) {
        m_obj = obj;
    }

    /**
     * Returns the session managed instance.
     * 
     * @return the instance
     */
    public T get() {
        return m_obj;
    }

    UUID getID() {
        return m_id;
    }

}
