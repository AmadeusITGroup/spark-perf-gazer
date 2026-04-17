package com.amadeus.perfgazer.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Documents a case class field for schema documentation generation.
 *
 * Used by the doc-generator to produce data model reference docs.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface ColumnDoc {
    /** Human-readable description of the field */
    String description();

    /** Optional unit of measure (e.g. "ms", "ns", "bytes") */
    String unit() default "";
}
