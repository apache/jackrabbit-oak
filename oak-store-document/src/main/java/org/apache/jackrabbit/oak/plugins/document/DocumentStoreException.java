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
package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * <code>DocumentStoreException</code> is a runtime exception for
 * {@code DocumentStore} implementations to signal unexpected problems like
 * a communication exception.
 */
public class DocumentStoreException extends RuntimeException {

    private static final long serialVersionUID = 4135594565927443068L;

    public enum Type {

        /**
         * A generic type of {@code DocumentStoreException}. This type is used
         * when no explicit type is given when a {@code DocumentStoreException}
         * is constructed.
         */
        GENERIC,

        /**
         * A {@code DocumentStoreException} caused by a transient problem. E.g.
         * a network issue. This type of exception indicates a future invocation
         * of the same operation may succeed if the underlying problem gets
         * resolved.
         */
        TRANSIENT
    }

    private final Type type;

    /**
     * Creates a {@link Type#GENERIC} {@code DocumentStoreException} with the
     * given message.
     *
     * @param message the exception message.
     */
    public DocumentStoreException(String message) {
        this(message, null);
    }

    /**
     * Creates a {@link Type#GENERIC} {@code DocumentStoreException} with the
     * given cause. The message of the exception is the value returned by
     * {@code cause.toString()} if available, otherwise {@code null}.
     *
     * @param cause the cause or {@code null} if nonexistent or unknown.
     */
    public DocumentStoreException(Throwable cause) {
        this(getMessage(cause), cause);
    }

    /**
     * Creates a {@link Type#GENERIC} {@code DocumentStoreException} with the
     * given message and cause.
     *
     * @param message the exception message.
     * @param cause the cause or {@code null} if nonexistent or unknown.
     */
    public DocumentStoreException(String message, Throwable cause) {
        this(message, cause, Type.GENERIC);
    }

    /**
     * Creates a {@code DocumentStoreException} with the given message, cause
     * and type.
     *
     * @param message the exception message.
     * @param cause the cause or {@code null} if nonexistent or unknown.
     * @param type the type of this exception.
     */
    public DocumentStoreException(String message, Throwable cause, Type type) {
        super(message, cause);
        this.type = requireNonNull(type);
    }

    /**
     * Converts the given {@code Throwable} into a {@link Type#GENERIC}
     * {@code DocumentStoreException}. If the {@code Throwable} is an instance
     * of {@code DocumentStoreException} this method returns the given
     * {@code Throwable} as is, otherwise it will be used as the cause of the
     * returned {@code DocumentStoreException}. The returned
     * {@code DocumentStoreException} will have the same message as the given
     * {@code Throwable}.
     *
     * @param t a {@code Throwable}.
     * @return a {@link Type#GENERIC} DocumentStoreException.
     */
    public static DocumentStoreException convert(@NotNull Throwable t) {
        return convert(t, t.getMessage());
    }

    /**
     * Converts the given {@code Throwable} into a {@link Type#GENERIC}
     * {@code DocumentStoreException}. If the {@code Throwable} is an instance
     * of {@code DocumentStoreException} this method returns the given
     * {@code Throwable} as is, otherwise it will be used as the cause of the
     * returned {@code DocumentStoreException}. The returned
     * {@code DocumentStoreException} will have the given message, unless the
     * {@code Throwable} already is a {@code DocumentStoreException}.
     *
     * @param t a {@code Throwable}.
     * @param msg a message for the {@code DocumentStoreException}.
     * @return a {@link Type#GENERIC} DocumentStoreException.
     */
    public static DocumentStoreException convert(@NotNull Throwable t, String msg) {
        return asDocumentStoreException(msg, t, Type.GENERIC, emptyList());
    }

    /**
     * Converts the given {@code Throwable} into a {@link Type#GENERIC}
     * {@code DocumentStoreException}. If the {@code Throwable} is an instance
     * of {@code DocumentStoreException} this method returns the given
     * {@code Throwable} as is, otherwise it will be used as the cause of the
     * returned {@code DocumentStoreException}. The returned
     * {@code DocumentStoreException} will have the same message as the given
     * {@code Throwable} appended with the list of {@code ids}.
     *
     * @param t a {@code Throwable}.
     * @param ids a list of {@code DocumentStore} IDs associated with the
     *            operation that triggered this exception.
     * @return a {@link Type#GENERIC} DocumentStoreException.
     */
    public static DocumentStoreException convert(@NotNull Throwable t,
                                                 Iterable<String> ids) {
        return asDocumentStoreException(t.getMessage(), t, Type.GENERIC, ids);
    }

    /**
     * Converts the given {@code Throwable} into a {@code DocumentStoreException}.
     * If the {@code Throwable} is an instance of {@code DocumentStoreException}
     * this method returns the given {@code Throwable} as is, otherwise it will
     * be used as the cause of the returned {@code DocumentStoreException}.
     * The {@code ids} will be appended to the given {@code message} and used
     * for the returned {@code DocumentStoreException}.
     *
     * @param message a message for the {@code DocumentStoreException}.
     * @param t a {@code Throwable}.
     * @param type the type of this exception.
     * @param ids a list of {@code DocumentStore} IDs associated with the
     *            operation that triggered this exception.
     * @return a {@link Type#GENERIC} DocumentStoreException.
     */
    public static DocumentStoreException asDocumentStoreException(String message,
                                                                  Throwable t,
                                                                  Type type,
                                                                  Iterable<String> ids) {
        String msg = message;
        if (ids.iterator().hasNext()) {
            msg += " " + CollectionUtils.toList(ids);
        }
        if (t instanceof DocumentStoreException) {
            return (DocumentStoreException) t;
        } else {
            return new DocumentStoreException(msg, t, type);
        }
    }

    /**
     * @return the type of this exception.
     */
    public Type getType() {
        return type;
    }

    @Nullable
    private static String getMessage(Throwable t) {
        return t == null ? null : t.toString();
    }
}
