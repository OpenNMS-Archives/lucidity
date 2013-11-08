package com.opennms.cassandra.mapper;


import java.util.UUID;


/**
 * Represents a session of a persisted object.
 *
 * @author eevans
 * @param <T>
 *            persisted type
 */
// FIXME sw: don't need this, m_objectCache caches only instances which already have a uuid, and the cache could be something like this
    /*
      cache {
        Class entityClass
        Schema entitySchema
        Map<UUID,entity> objectCache
      }
     */
public class Session<T> {

    public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.ONE;

    private final UUID m_id = UUID.randomUUID();
    private final T m_obj;

    // FIXME sw: do we need it here? doesn't seem to be related to the livecycle of an entity and EntityStore has method arguments for non default behaviour
    private ConsistencyLevel m_consistency = ConsistencyLevel.ONE;

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

    public ConsistencyLevel getConsistencyLevel() {
        return m_consistency;
    }

    public Session<T> setConsistencyLevel(ConsistencyLevel cl) {
        m_consistency = cl;
        return this;
    }

}
