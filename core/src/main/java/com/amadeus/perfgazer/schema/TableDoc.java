package com.amadeus.perfgazer.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a Report case class and binds it to its SQL view name.
 *
 * Used by the doc-generator to produce data model reference docs.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TableDoc {
    /** The SQL temporary view name (e.g. "job", "sql", "stage", "task") */
    String name();

    /** Human-readable description of the report/view */
    String description();
}
