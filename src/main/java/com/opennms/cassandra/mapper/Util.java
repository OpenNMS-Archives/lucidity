package com.opennms.cassandra.mapper;


import static com.google.common.base.Throwables.propagate;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;


class Util {

    /**
     * Convenience method; Wraps {@link Field#get(Object)} to propagate
     * {@link RuntimeException}s.
     *
     */
    static Object getFieldValue(Field f, Object obj) {
        try {
            return f.get(obj);
        }
        catch (IllegalArgumentException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Convenience method; Wraps {@link Field#set(Object, Object)} to propagate
     * {@link RuntimeException}s.
     *
     */
    static void setFieldValue(Field f, Object target, Object value) {
        try {
            f.set(target, value);
        }
        catch (IllegalArgumentException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return a class's no-argument constructor, or {@link Optional#absent()} no such constructor
     * exists.
     *
     * @param cls
     *            the Class
     * @return Optional no-argument constructor
     */
    static <T> Optional<Constructor<T>> getNoArgConstructor(Class<T> cls) {
        Constructor<T> ctor;
        try {
            ctor = cls.getDeclaredConstructor();
        }
        catch (NoSuchMethodException | SecurityException e) {
            return Optional.absent();
        }
        return Optional.of(ctor);
    }

    /**
     * Convenience method; Returns a new instance of a class from a nullary constructor. Exceptions
     * are propagated as {@link RuntimeException}s.
     *
     * @param cls
     *            class to create new instance from
     * @return instance
     */
    static <T> T newInstance(Class<T> cls) {
        try {
            return cls.getDeclaredConstructor().newInstance();
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

}
