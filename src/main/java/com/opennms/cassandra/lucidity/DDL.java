package com.opennms.cassandra.lucidity;


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
