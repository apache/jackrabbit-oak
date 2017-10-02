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
package org.apache.jackrabbit.oak.spi.xml;

import javax.jcr.RepositoryException;
import javax.jcr.Value;

/**
 * {@code TextValue} represents a serialized property value read
 * from a System or Document View XML document.
 */
public interface TextValue {

    String getString();

    Value getValue(int targetType) throws RepositoryException;

    /**
     * Dispose this value, i.e. free all bound resources. Once a value has
     * been disposed, further method invocations will cause an IOException
     * to be thrown.
     */
    void dispose();

}