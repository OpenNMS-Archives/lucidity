package com.opennms.cassandra.mapper;

import java.util.UUID;

import com.google.common.base.Optional;

// FIXME: Javadoc all around.

public interface Storage {
    void create(Object obj);
    void update(Object obj);
    <T> Optional<T> read(Class<T> cls, UUID id);
    <T> Optional<T> read(Class<T> cls, String indexedName, Object value);
    void delete(Object obj);
}
