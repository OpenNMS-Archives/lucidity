package com.opennms.lucidity;


import java.io.Closeable;
import java.util.UUID;

import com.google.common.base.Optional;


public interface EntityStore extends Closeable {

    /**
     * Persist a new object using the default consistency level.
     * 
     * @param obj
     *            the object to persist
     * @return a tracked instance of the persisted object
     */
    <T> T create(T obj);

    /**
     * Persist a new object with the specified consistency level.
     * 
     * @param obj
     *            the object to persist
     * @param consistency
     *            consistency level to use
     * @return a tracked instance of the persisted object
     */
    <T> T create(T obj, ConsistencyLevel consistency);

    /**
     * Persists changes to an object using the default consistency level. The supplied argument must
     * be a tracked instance, an instance returned from either a <code>create(...)</code> or
     * <code>read(...)</code> method. A call to this method results in a Cassandra
     * <code>UPDATE</code> for only those attributes that have changed.
     * 
     * @param obj
     *            the object to update
     */
    <T> void update(T obj);

    /**
     * Persists changes to an object with the specified consistency level. The supplied argument
     * must be a tracked instance, an instance returned from either a <code>create(...)</code> or
     * <code>read(...)</code> method. A call to this method results in a Cassandra
     * <code>UPDATE</code> for only those attributes that have changed.
     * 
     * @param obj
     *            the object to update
     */
    <T> void update(T obj, ConsistencyLevel consistency);

    /**
     * Read an object by its ID using the default consistency level.
     * 
     * @param cls
     *            class of the object to read
     * @param id
     *            unique ID
     * @return an {@link Optional} of the requested object.
     */
    <T> Optional<T> read(Class<T> cls, UUID id);

    /**
     * Read an object by its ID using the specified consistency level.
     * 
     * @param cls
     *            class of the object to read
     * @param id
     *            unique ID
     * @param consistency
     *            consistency level to use
     * @return an {@link Optional} of the requested object.
     */
    <T> Optional<T> read(Class<T> cls, UUID id, ConsistencyLevel consistency);

    /**
     * Read an object by an indexed value, using the default consistency level.
     * 
     * @param cls
     *            class of the object to read
     * @param indexedName
     *            name of the indexed column
     * @param value
     *            value the column is expected to match
     * @return an {@link Optional} of the requested object.
     */
    <T> Optional<T> read(Class<T> cls, String indexedName, Object value);

    /**
     * Read an object by an indexed value, with the specified consistency level.
     * 
     * @param cls
     *            class of the object to read
     * @param indexedName
     *            name of the indexed column
     * @param value
     *            value the column is expected to match
     * @return an {@link Optional} of the requested object.
     */
    <T> Optional<T> read(Class<T> cls, String indexedName, Object value, ConsistencyLevel consistency);

    /**
     * Delete an object using the default consistency level.
     * 
     * @param obj
     *            the object to delete
     */
    <T> void delete(T obj);

    /**
     * Delete an object with the specified consistency level.
     * 
     * @param obj
     *            the object to delete
     */
    <T> void delete(T obj, ConsistencyLevel consistency);

}
