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

package org.apache.jackrabbit.oak.spi.adapter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An adapter manager allows objects to be converted to target class instances using registered AdapterFactory implementations.
 */
public interface AdapterManager {

    /**
     * Adapt from the cupplied object to an instance of the target class. If the adaption is not possible, return null.
     * @param o the source object.
     * @param targetClass the target class.
     * @param <T> the type of the target class.
     * @return an instance of the target class or null of the adaption cant be perfomed.
     */
    @Nullable
    <T> T adaptTo(@Nonnull Object o, @Nonnull Class<T> targetClass);

    /**
     * Only to be active in a non OSGi environment, otherwise throw an exception.
     * @param adapterFactory the adapterFactory to add.
     */
    void addAdapterFactory(AdapterFactory adapterFactory);

    /**
     * Only to be active in a non OSGi environment, otherwise throw an exception.
     * @param adapterFactory the adapterFactory to remove.
     */
    void removeAdapterFactory(AdapterFactory adapterFactory);
}
