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
