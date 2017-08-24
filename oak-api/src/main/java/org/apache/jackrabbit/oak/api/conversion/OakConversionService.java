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

package org.apache.jackrabbit.oak.api.conversion;

/**
 * OSGi Service to allow things in OSGi to convert objects.
 * TODO: Needs to be moved to the correct location for this type of API.
 */
public interface OakConversionService {

    /**
     * Convert from source to an instance of targetClass or return null if not possible.
     * @param source
     * @param targetClass
     * @param <T>
     * @return
     */
    <T> T convertTo(Object source, Class<T> targetClass);
}
