package com.opennms.cassandra.lucidity.annotations;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Is used to specify a mapped column for a persistent field.
 * If no Column annotation is specified, the default values are applied.
 * <p> Examples:
 *
 * <blockquote><pre>
 * Example:
 * &#064;Column(name="email")
 * private String emailAddress;
 * </pre></blockquote>
 */ 
@Target({ FIELD }) 
@Retention(RUNTIME)
public @interface Column {

    /**
     * (Optional) The name of the column. Defaults to 
     * the property or field name.
     */
    String name() default "";

}
