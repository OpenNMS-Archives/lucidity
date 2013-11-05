package com.opennms.cassandra.mapper;


import java.lang.reflect.Field;

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

}
