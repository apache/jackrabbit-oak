/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.api;

import static java.lang.String.format;

/**
 * Main exception thrown by methods defined on the {@code ContentSession}
 * interface indicating that committing a given set of changes failed.
 */
public class CommitFailedException extends Exception {

    /**
     * Source name for exceptions thrown by components in the Oak project.
     */
    public static final String OAK = "Oak";

    /**
     * Type name for access violation (i.e. permission denied) errors.
     */
    public static final String ACCESS = "Access";

    /**
     * Type name for constraint violation errors.
     */
    public static final String CONSTRAINT = "Constraint";

    /** Serial version UID */
    private static final long serialVersionUID = 2727602333350620918L;

    private final String source;

    private final String type;

    private final int code;

    public CommitFailedException(
            String source, String type, int code, String message,
            Throwable cause) {
        super(format("%s%s%04d: %s", source, type, code, message), cause);
        this.source = source;
        this.type = type;
        this.code = code;
    }

    public CommitFailedException(
            String type, int code, String message, Throwable cause) {
        this(OAK, type, code, message, cause);
    }

    public CommitFailedException(String type, int code, String message) {
        this(type, code, message, null);
    }

    /**
     * Checks whether this exception is of the given type.
     *
     * @param type type name
     * @return {@code true} iff this exception is of the given type
     */
    public boolean isOfType(String type) {
        return this.type.equals(type);
    }

    /**
     * Checks whether this is an access violation exception.
     *
     * @return {@code true} iff this is an access violation exception
     */
    public boolean isAccessViolation() {
        return isOfType(ACCESS);
    }

    /**
     * Checks whether this is a constraint violation exception.
     *
     * @return {@code true} iff this is a constraint violation exception
     */
    public boolean isConstraintViolation() {
        return isOfType(CONSTRAINT);
    }

    /**
     * Returns the name of the source of this exception.
     *
     * @return source name
     */
    public String getSource() {
        return source;
    }

    /**
     * Return the name of the type of this exception.
     *
     * @return type name
     */
    public String getType() {
        return type;
    }

    /**
     * Returns the type-specific error code of this exception.
     *
     * @return error code
     */
    public int getCode() {
        return code;
    }

}
