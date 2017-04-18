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

import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.InvalidItemStateException;
import javax.jcr.NamespaceException;
import javax.jcr.ReferentialIntegrityException;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.security.AccessControlException;
import javax.jcr.version.LabelExistsVersionException;
import javax.jcr.version.VersionException;

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
     * Type name for access control violation errors.
     */
    public static final String ACCESS_CONTROL = "AccessControl";

    /**
     * Type name for constraint violation errors.
     */
    public static final String CONSTRAINT = "Constraint";

    /**
     * Type name for referential integrity violation errors.
     */
    public static final String INTEGRITY = "Integrity";

    /**
     * Type name for lock violation errors.
     */
    public static final String LOCK = "Lock";

    /**
     * Type name for name violation errors.
     */
    public static final String NAME = "Name";

    /**
     * Type name for namespace violation errors.
     */
    public static final String NAMESPACE = "Namespace";

    /**
     * Type name for node type violation errors.
     */
    public static final String NODE_TYPE = "NodeType";

    /**
     * Type name for state violation errors.
     */
    public static final String STATE = "State";

    /**
     * Type name for version violation errors.
     */
    public static final String VERSION = "Version";

    /**
     * Type name for label exists version errors.
     */
    public static final String LABEL_EXISTS = "LabelExists";

    /**
     * Type name for merge errors.
     */
    public static final String MERGE = "Merge";

    /**
     * Unsupported operation or feature
     */
    public static final String UNSUPPORTED = "Unsupported";

    /**
     * Serial version UID
     */
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
     * Checks whether this is an access control violation exception.
     *
     * @return {@code true} iff this is an access control violation exception
     */
    public boolean isAccessControlViolation() {
        return isOfType(ACCESS_CONTROL);
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

    /**
     * Wraps the given {@link CommitFailedException} instance using the
     * appropriate {@link javax.jcr.RepositoryException} subclass based on the
     * {@link CommitFailedException#getType() type} of the given exception.
     *
     * @return matching repository exception
     */
    public RepositoryException asRepositoryException() {
        return asRepositoryException(this.getMessage());
    }

    /**
     * Wraps the given {@link CommitFailedException} instance using the
     * appropriate {@link javax.jcr.RepositoryException} subclass based on the
     * {@link CommitFailedException#getType() type} of the given exception.
     *
     * @param message The exception message.
     * @return matching repository exception
     */
    public RepositoryException asRepositoryException(@Nonnull String message) {
        if (isConstraintViolation()) {
            return new ConstraintViolationException(message, this);
        } else if (isOfType(NAMESPACE)) {
            return new NamespaceException(message, this);
        } else if (isOfType(NODE_TYPE)) {
            return new NoSuchNodeTypeException(message, this);
        } else if (isAccessViolation()) {
            return new AccessDeniedException(message, this);
        } else if (isAccessControlViolation()) {
            return new AccessControlException(message, this);
        } else if (isOfType(INTEGRITY)) {
            return new ReferentialIntegrityException(message, this);
        } else if (isOfType(STATE)) {
            return new InvalidItemStateException(message, this);
        } else if (isOfType(MERGE)) {
            return new InvalidItemStateException(message, this);
        } else if (isOfType(VERSION)) {
            return new VersionException(message, this);
        } else if (isOfType(LABEL_EXISTS)) {
            return new LabelExistsVersionException(message, this);
        } else if (isOfType(LOCK)) {
            return new LockException(message, this);
        } else if (isOfType(UNSUPPORTED)) {
            return new UnsupportedRepositoryOperationException(message, this);
        } else {
            return new RepositoryException(message, this);
        }
    }
}
