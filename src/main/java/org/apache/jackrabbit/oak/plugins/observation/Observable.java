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

package org.apache.jackrabbit.oak.plugins.observation;

import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher.Listener;

/**
 * An {@code Observable} supports attaching {@link Listener} instances for
 * listening to changes in a {@code ContentRepository}.
 * @see ChangeDispatcher
 */
public interface Observable {

    /**
     * Register a new {@code Listener}. Clients need to call
     * {@link ChangeDispatcher.Listener#dispose()} when to free
     * up any resources associated with the listener when done.
     * @return a fresh {@code Listener} instance.
     */
    Listener newListener();
}
