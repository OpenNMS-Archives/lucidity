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
package com.opennms.lucidity.annotations;


import java.lang.annotation.Target;
import java.lang.annotation.Retention;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * This annotation specifies the table for the annotated entity
 * <p>
 * If no <code>Table</code> annotation is specified for an entity, the name of the entity class is
 * used as the name.
 *
 * <pre>
 *    Example:
 * 
 *    &#064;Entity
 *    &#064;Table(name="users")
 *    public class User { ... }
 * </pre>
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface Table {

    /**
     * (Optional) The name of the table.
     * <p>
     * Defaults to the entity name.
     */
    String name() default "";

}
