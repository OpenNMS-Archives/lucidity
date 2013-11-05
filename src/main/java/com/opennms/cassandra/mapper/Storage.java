package com.opennms.cassandra.mapper;

import java.util.UUID;

// FIXME: Javadoc all around.

public interface Storage {
    UUID create(Object obj);
    void update(Object obj);
    <T> T read(Class<T> cls, UUID id) throws StorageException;
    <T> T read(Class<T> cls, String indexedName, Object value) throws StorageException;
    void delete(Object obj);
}
