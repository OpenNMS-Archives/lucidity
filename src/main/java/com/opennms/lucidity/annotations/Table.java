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
