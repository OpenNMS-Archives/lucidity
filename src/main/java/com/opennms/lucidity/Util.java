/*
 * Copyright 2013, The OpenNMS Group
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.opennms.lucidity;


import static com.google.common.base.Throwables.propagate;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

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

    static Type[] getParameterizedTypes(Field f) {
        return ((ParameterizedType)f.getGenericType()).getActualTypeArguments();
    }

}
