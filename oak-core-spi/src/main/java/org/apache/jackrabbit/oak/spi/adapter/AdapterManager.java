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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The AdapterManager maintains a map of lists of AdapterFactories keyed by target class.
 * For simplicity in this PoC its going to be a Singleton, but if the container is always OSGi it
 * could be an OSGi Component.
 */
public class AdapterManager {

    private static AdapterManager singletonAdapterManager = new AdapterManager();

    public static AdapterManager getInstance() {
        return singletonAdapterManager;
    }

    private AdapterManager() {

    }

    private Map<String, List<AdapterFactory>> adapterFactories = new ConcurrentHashMap<String, List<AdapterFactory>>();

    public <T> T getAdapter(@Nonnull  Object o, @Nonnull Class<T> targetClass) {
        List<AdapterFactory> possibleAdapters = adapterFactories.get(targetClass.getName());
        for ( AdapterFactory af : possibleAdapters) {
            T target = af.adaptTo(o, targetClass);
            if ( target != null) {
                return target;
            }
        }
        return null;
    }


    public synchronized void addAdapterFactory(AdapterFactory adapterFactory) {
        for( String className: adapterFactory.getTargetClasses()) {
            List<AdapterFactory> possibleAdapters = adapterFactories.get(className);
            if ( possibleAdapters == null) {
                possibleAdapters = new ArrayList<>();
                adapterFactories.put(className, possibleAdapters);
            }
            possibleAdapters.add(adapterFactory);
            Collections.sort(possibleAdapters, new Comparator<AdapterFactory>() {
                @Override
                public int compare(AdapterFactory o1, AdapterFactory o2) {
                    return o1.getPriority() - o2.getPriority();
                }
            });
        }
    }

    public synchronized void removeAdapterFactory(AdapterFactory adapterFactory) {
        for( String className: adapterFactory.getTargetClasses()) {
            List<AdapterFactory> possibleAdapters = adapterFactories.get(className);
            if (possibleAdapters != null) {
                possibleAdapters.remove(adapterFactory);
            }
        }

    }
}
