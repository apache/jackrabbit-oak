/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.commons.annotations;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PACKAGE;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.CLASS;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Elements annotated @Internal are -- although possibly exported -- intended
 * for Oak's internal use only. Such elements are not public by design and
 * likely to be removed, have their signature change, or have their access level
 * decreased in future versions without notice. {@code @Internal} elements are
 * eligible for immediate modification or removal and are not subject to any
 * policies with respect to deprecation.
 * <p>
 * Note that Oak APIs are considered internal use by default, unless the package
 * they appear in is annotated with a
 * {@link org.osgi.annotation.versioning.Version} annotation with a value
 * greater than "1.0.0".
 */
@Documented
@Retention(CLASS)
@Target({ TYPE, METHOD, CONSTRUCTOR, ANNOTATION_TYPE, PACKAGE })
public @interface Internal {
    /**
     * @return (optional) reason for being internal
     */
    String reason() default "";

    /**
     * @return (optional) first package version making this API internal
     */
    String since() default "";
}
