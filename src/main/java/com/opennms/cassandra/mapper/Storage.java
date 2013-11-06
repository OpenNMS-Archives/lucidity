package com.opennms.cassandra.mapper;

import java.util.UUID;

import com.google.common.base.Optional;

public interface Storage {

    /**
     * Persist a new object.
     * <p>
     * Unique IDs are generated for objects at creation-time. This ID will be assigned to the passed
     * object reference on success; It is considered an error to pass an object with a non-null ID
     * value.
     * </p>
     *
     * @param obj
     *            the object to persist
     */
    void create(Object obj);

    /**
     * Update an existing object.
     * <p>
     * This method attempts to persist only those properties that have changed. To accomplish this,
     * implementations track references to objects seen in {@link #create(Object)}, and
     * {@link #read(Class, UUID)} to use for comparison at update-time. As a result, objects passed
     * to this method must have been used in {@link #create(Object)}, or returned by
     * {@link #read(Class, UUID)}, an {@link IllegalStateException} exception is raised otherwise.
     * </p>
     *
     * @param obj
     *            the object to update
     */
    void update(Object obj);

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
     * Delete an object.
     *
     * @param obj
     *            the object to delete
     */
    void delete(Object obj);

}
