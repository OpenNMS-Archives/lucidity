package com.opennms.lucidity.annotations;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Defines a many-valued association with one-to-many multiplicity.
 *
 * <pre>
 *
 *    Example 1:
 *
 *    &#064;OneToMany
 *    private Set&lt;Address&gt; addresses;
 *
 * </pre>
 *
 */
@Target({ FIELD }) 
@Retention(RUNTIME)
public @interface OneToMany {

}

