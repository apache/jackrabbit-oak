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
package org.apache.jackrabbit.oak.plugins.index.solr.configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;

/**
 * Configuration parameters for starting a {@link org.apache.solr.client.solrj.SolrClient}
 */
public abstract class SolrServerConfiguration<S extends SolrServerProvider> {

    private final Type type;
    private volatile Constructor<?> constructor;

    protected SolrServerConfiguration() {
        Type superclass = getClass().getGenericSuperclass();
        if (superclass instanceof Class) {
            throw new RuntimeException("Missing type parameter.");
        }
        this.type = ((ParameterizedType) superclass).getActualTypeArguments()[0];
    }

    public Type getType() {
        return this.type;
    }

    public S getProvider()
            throws IllegalAccessException,
            InvocationTargetException, InstantiationException {
        if (constructor == null) {
            Class<?> rawType = type instanceof Class<?>
                    ? (Class<?>) type
                    : (Class<?>) ((ParameterizedType) type).getRawType();
            Constructor<?>[] constructors = rawType.getConstructors();
            for (Constructor<?> c: constructors) {
                if (c.getParameterTypes().length == 1 && c.getParameterTypes()[0].equals(this.getClass())) {
                    constructor = c;
                }
            }
            if (constructor == null) {
                throw new InstantiationException("missing constructor SolrServerProvider(SolrServerConfiguration) for type "+ rawType);
            }
        }
        return (S) constructor.newInstance(this); // TODO : each SolrServerProvider impl. is forced to have a constructor with a SolrServerConfiguration, fix?
    }

}
