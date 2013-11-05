package com.opennms.cassandra.mapper;


import javax.persistence.Entity;

import org.junit.Test;


public class SchemaTest {

    @Entity
    private class InvalidConstructor {
        @SuppressWarnings("unused")
        InvalidConstructor(String argument) {

        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoArgConstructor() {
        Schema.fromClass(InvalidConstructor.class);
    }

}
