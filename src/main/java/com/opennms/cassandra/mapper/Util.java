package com.opennms.cassandra.mapper;


import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;


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
    static Optional<Constructor<?>> getNoArgConstructor(Class<?> cls) {
        return Iterables.tryFind(Arrays.asList(cls.getDeclaredConstructors()), new Predicate<Constructor<?>>() {

            @Override
            public boolean apply(Constructor<?> input) {
                return input.getParameterTypes().length == 0;
            }
        });
    }

}
