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

package org.apache.jackrabbit.oak.spi.commit;

import java.io.Closeable;

/**
 * An {@code Observable} supports attaching {@link Observer} instances for
 * listening to content changes.
 *
 * @see Observable
 */
public interface Observable {

    /**
     * Register a new {@code Observer}. Clients need to call {@link Closeable#close()} 
     * to stop getting notifications on the registered observer and to free up any resources
     * associated with the registration.
     * 
     * @return a {@code Closeable} instance.
     */
    Closeable addObserver(Observer observer);
}
