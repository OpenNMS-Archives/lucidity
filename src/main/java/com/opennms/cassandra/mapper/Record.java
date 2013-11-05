package com.opennms.cassandra.mapper;


import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;


class Record {

    Object m_idValue;
    Map<String, Object> m_columns = Maps.newHashMap();
    Map<Field, Collection<?>> m_oneToManys = Maps.newHashMap();

    Record(Object idValue) {
        m_idValue = idValue;
    }

    Object getIDValue() {
        return m_idValue;
    }

    Map<String, Object> getColumns() {
        return m_columns;
    }

    void putColumn(String name, Object value) {
        m_columns.put(name, value);
    }

    Map<Field, Collection<?>> getOneToManys() {
        return m_oneToManys;
    }

    void putOneToMany(Field field, Collection<?> relations) {
        m_oneToManys.put(field, relations);
    }

}
