package com.opennms.lucidity;


import java.io.Closeable;
import java.util.UUID;

import com.google.common.base.Optional;

// FIXME: re-javadoc

public interface EntityStore extends Closeable {

    /**
     * Persist a new object.
     * 
     * @param obj
     *            the object to persist
     * @return a newly created session for the object
     */
    <T> T create(T obj);

    /**
     * Persist a new object using a given consistency level.
     * 
     * @param obj
     *            the object to persist
     * @param consistency
     *            consistency level to use
     * @return a newly created session for the object
     */
    <T> T create(T obj, ConsistencyLevel consistency);

    /**
     * Persists changes to an object.
     *
     * @param obj
     *            the object to update
     */
    <T> void update(T obj);
    
    /**
     * Persists changes to an object.
     * 
     * @param obj
     *            the object to update
     */
    <T> void update(T obj, ConsistencyLevel consistency);

    /**
     * Read an object by its ID.
     * 
     * @param cls
     *            class of the object to read
     * @param id
     *            unique ID
     * @return the requested object
     */
    <T> Optional<T> read(Class<T> cls, UUID id);

    /**
     * Read an object by its ID.
     * 
     * @param cls
     *            class of the object to read
     * @param id
     *            unique ID
     * @param consistency
     *            consistency level to use
     * @return the requested object
     */
    <T> Optional<T> read(Class<T> cls, UUID id, ConsistencyLevel consistency);

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
    <T> Optional<T> read(Class<T> cls, String indexedName, Object value);

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
    <T> Optional<T> read(Class<T> cls, String indexedName, Object value, ConsistencyLevel consistency);

    /**
     * Delete an object.
     * 
     * @param obj
     *            the object to delete
     */
    <T> void delete(T obj);
    
    /**
     * Delete an object.
     * 
     * @param obj
     *            the object to delete
     */
    <T> void delete(T obj, ConsistencyLevel consistency);

}
