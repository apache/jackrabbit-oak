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

import javax.jcr.RepositoryException;

/**
 * Main exception thrown by methods defined on the {@code ContentSession} interface
 * indicating that committing a given set of changes failed.
 */
public class CommitFailedException extends Exception {
    public CommitFailedException() {
    }

    public CommitFailedException(String message) {
        super(message);
    }

    public CommitFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public CommitFailedException(Throwable cause) {
        super(cause);
    }

    /**
     * Rethrow this exception cast into a {@link RepositoryException}: if the cause
     * for this exception already is a {@code RepositoryException}, this exception is
     * wrapped into the actual type of the cause and rethrown. If creating an instance
     * of the actual type of the cause fails, cause is simply re-thrown.
     * If the cause for this exception is not a {@code RepositoryException} then  a
     * new {@code RepositoryException} instance with this {@code CommitFailedException}
     * is thrown.
     * @throws RepositoryException
     */
    public void throwRepositoryException() throws RepositoryException {
        Throwable cause = getCause();
        if (cause instanceof RepositoryException) {
            RepositoryException e;
            try {
                // Try to preserve all parts of the stack trace
                e = (RepositoryException) cause.getClass().getConstructor().newInstance();
                e.initCause(this);
            }
            catch (Exception ex) {
                // Fall back to the initial cause on failure
                e = (RepositoryException) cause;
            }

            throw e;
        }
        else {
            throw new RepositoryException(this);
        }
    }
}
