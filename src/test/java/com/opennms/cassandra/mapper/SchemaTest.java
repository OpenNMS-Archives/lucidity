package com.opennms.cassandra.mapper;


import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.junit.Test;


public class SchemaTest {

    @Entity
    @Table(name = "points")
    private class Point {
        @Id
        private UUID m_id;
        @Column(name = "x")
        private double m_x;
        @Column(name = "y")
        private double m_y;

        private Point(double x, double y) {
            m_x = x;
            m_y = y;
        }
    }

    @Test
    public void test() {
        System.err.println(Schema.fromClass(Point.class));
    }

    @Test
    public void testOneToMany() {
        System.err.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.err.println(Schema.fromClass(Address.class).toDDL());
        System.err.println(Schema.fromClass(User.class).toDDL());
        System.err.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");    
    }

}
