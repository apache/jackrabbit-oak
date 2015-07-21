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

package org.apache.jackrabbit.oak.plugins.document.util;

import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.jackrabbit.oak.plugins.document.Revision;

/**
 * Experimental extension point for OAK-1772 to try out alternative approaches
 * for persisting in memory state. Not part of API.
 */
public abstract class MapFactory {

    private static boolean useMemoryMapFactory() {
        return Boolean.getBoolean("oak.useMemoryMapFactory");
    }

    private static boolean useHybridMapFactory() {
        return Boolean.getBoolean("oak.useHybridMapFactory");
    }

    public static final MapFactory DEFAULT = new MapFactory() {
        @Override
        public ConcurrentMap<String, Revision> create() {
            return new ConcurrentHashMap<String, Revision>();
        }

        @Override
        public ConcurrentMap<String, Revision> create(Comparator<String> comparator) {
            return new ConcurrentSkipListMap<String, Revision>(comparator);
        }

        @Override
        public void dispose() {
            // nothing to do
        }
    };

    public abstract ConcurrentMap<String, Revision> create();

    public abstract ConcurrentMap<String, Revision> create(Comparator<String> comparator);

    public abstract void dispose();

    private static MapFactory instance = DEFAULT;

    public static MapFactory getInstance(){
        return instance;
    }

    public static void setInstance(MapFactory instance) {
        MapFactory.instance = instance;
    }

    public static MapFactory createFactory() {
        if (useMemoryMapFactory()) {
            return MapFactory.getInstance();
        } else if (useHybridMapFactory()) {
            return new HybridMapFactory();
        } else {
            return new MapDBMapFactory();
        }
    }
}
