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


import java.util.Set;

import org.reflections.Reflections;


public class DDL {

    private final Set<Class<?>> m_entities;

    public DDL(String prefix) {
        Reflections reflections = new Reflections(prefix);
        m_entities = reflections.getTypesAnnotatedWith(Schema.ENTITY);
    }

    @Override
    public String toString() {

        StringBuilder s = new StringBuilder();

        for (Class<?> cls : m_entities) {
            s.append(Schema.fromClass(cls).toDDL());
        }

        return s.toString();
    }

    public static void main(String... args) {

        if (!(args.length == 1)) {
            System.err.printf("Usage: java %s <package prefix>%n", DDL.class.getCanonicalName());
            System.exit(1);
        }

        System.out.println(new DDL(args[0]));

        System.exit(0);

    }

}
