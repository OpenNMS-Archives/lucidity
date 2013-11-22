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


import static com.opennms.lucidity.annotations.IndexType.INVERTED;

import java.io.PrintStream;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.opennms.lucidity.annotations.Column;
import com.opennms.lucidity.annotations.EmbeddedCollection;
import com.opennms.lucidity.annotations.Entity;
import com.opennms.lucidity.annotations.Id;
import com.opennms.lucidity.annotations.Index;


public class SchemaTest {

    @Entity static class InvalidConstructor {
        InvalidConstructor(String argument) {}
    }

    @Entity static class InvalidID {
        @Id private String id;
        @Column String name;
    }

    @Entity static class InvalidColumn {
        @Id private UUID id;
        @Column PrintStream stream;
    }

    @Entity static class NoColumn {
        @Id private UUID id;
    }

    @Entity static class WithIndexedMap {
        @Id private UUID id;
        @Index(type=INVERTED) @EmbeddedCollection @Column Map<String, String> food;
    }

    @Entity static class WithBadMapType {
        @Id private UUID id;
        @EmbeddedCollection @Column Map<String, Runnable> runners;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoArgConstructor() {
        Schema.fromClass(InvalidConstructor.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIDType() {
        Schema.fromClass(InvalidID.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadType() {
        Schema.fromClass(InvalidColumn.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingColumn() {
        Schema.fromClass(NoColumn.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithIndexedMap() {
        Schema.fromClass(WithIndexedMap.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithBadMapType() {
        Schema.fromClass(WithBadMapType.class);
    }

}
