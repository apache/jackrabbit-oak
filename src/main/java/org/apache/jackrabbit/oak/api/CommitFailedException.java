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
     * Throw this exception wrapped into a {@link RepositoryException}
     * @throws RepositoryException
     */
    public void throwRepositoryException() throws RepositoryException {
        throw new RepositoryException(this);
    }
}
