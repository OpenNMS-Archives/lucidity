package com.opennms.lucidity.annotations;


import java.lang.annotation.Target;
import java.lang.annotation.Retention;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * Specifies that the class is an entity. This annotation is applied to the entity class.
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface Entity {

}
