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

/**
 * This exception can be thrown by implementers of this API to signal an error
 * condition caused by an invalid state of the repository.
 * <p>
 * It's up to the implementation to distinguish between recoverable and
 * unrecoverable error conditions. In case of recoverable error conditions, it's
 * appropriate for an implementation to create a subclass of this exception and
 * expose that subclass as part of its public API. This way, clients of the
 * Content Repository API can catch specific failures, provided that they also
 * want to introduce a dependency to the implementation's API.
 */
public class IllegalRepositoryStateException extends RuntimeException {

    public IllegalRepositoryStateException() {
    }

    public IllegalRepositoryStateException(String message) {
        super(message);
    }

    public IllegalRepositoryStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalRepositoryStateException(Throwable cause) {
        super(cause);
    }

}
