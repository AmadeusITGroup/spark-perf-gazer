package com.amadeus.sparklear.annotations;

import java.lang.annotation.*;

/**
  * Unstable APIs, with no guarantee on stability.
  * Classes that are unannotated are considered Unstable.
  */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER,
  ElementType.CONSTRUCTOR, ElementType.LOCAL_VARIABLE, ElementType.PACKAGE})
public @interface Unstable {}
