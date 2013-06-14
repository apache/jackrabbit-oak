/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.whiteboard;

import java.util.Map;

public class DefaultWhiteboard implements Whiteboard {

    @Override
    public <T> Registration register(
            final Class<T> type, final T service, final Map<?, ?> properties) {
        registered(type, service, properties);
        return new Registration() {
            @Override
            public void unregister() {
                unregistered(type, service, properties);
            }
        };
    }

    //---------------------------------------------------------< protected >--

    protected void registered(
            Class<?> type, Object service, Map<?, ?> properties) {
    }

    protected void unregistered(
            Class<?> type, Object service, Map<?, ?> properties) {
    }

}
