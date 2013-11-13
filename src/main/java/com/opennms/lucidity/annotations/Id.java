package com.opennms.lucidity.annotations;


import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;


/**
 * Specifies the primary key field of an entity.
 * 
 * <pre>
 *   Example:
 * 
 *   &#064;Id
 *   private UUID id;
 * </pre>
 */
@Target({ FIELD })
@Retention(RUNTIME)
public @interface Id {

}
