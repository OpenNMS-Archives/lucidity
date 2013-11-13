package com.opennms.lucidity;


import org.junit.Test;

import com.opennms.lucidity.Schema;
import com.opennms.lucidity.annotations.Entity;
import com.opennms.lucidity.annotations.Id;


public class SchemaTest {

    @Entity private class InvalidConstructor {
        @SuppressWarnings("unused")
        InvalidConstructor(String argument) {

        }
    }

    @Entity private class InvalidID {
        @Id private String id;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoArgConstructor() {
        Schema.fromClass(InvalidConstructor.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIDType() {
        Schema.fromClass(InvalidID.class);
    }

}
