package com.opennms.cassandra.mapper;


import java.util.UUID;


public interface Storage {

    /**
     * Persist a new object.
     *
     * @param obj
     *            the object to persist
     * @return a newly created session for the object
     */
    <T> Session<T> create(T obj);

    /**
     * Persists changes to an object.
     *
     * @param obj
     *            the object to update
     */
    <T> void update(Session<T> obj);

    /**
     * Read an object by its ID.
     *
     * @param cls
     *            class of the object to read
     * @param id
     *            unique ID
     * @return the requested object
     */
    <T> Session<T> read(Class<T> cls, UUID id);

    /**
     * Read an object by an indexed value.
     *
     * @param cls
     *            class of the object to read
     * @param indexedName
     *            name of the indexed column
     * @param value
     *            value the column is expected to match
     * @return the requested object
     */
    <T> Session<T> read(Class<T> cls, String indexedName, Object value);

    /**
     * Delete an object.
     *
     * @param obj
     *            the object to delete
     */
    <T> void delete(Session<T> obj);

}
