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
package org.apache.jackrabbit.oak.coversion;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.ReferencePolicyOption;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoProvider;
import org.apache.jackrabbit.oak.spi.adapter.AdapterFactory;
import org.apache.jackrabbit.oak.spi.adapter.AdapterManager;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The AdapterManager maintains a map of lists of AdapterFactories keyed by target class.
 */
@Component(immediate = true)
@Service(AdapterManager.class)
public class AdapterManagerImpl implements AdapterManager {


    public AdapterManagerImpl() {
    }




    @Reference(policy = ReferencePolicy.DYNAMIC,
            cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
            policyOption = ReferencePolicyOption.GREEDY,
            referenceInterface = AdapterFactory.class,
            bind = "addAdapterFactory",
            unbind = "removeAdapterFactory"
    )
    private Map<String, List<AdapterFactory>> adapterFactories = new ConcurrentHashMap<String, List<AdapterFactory>>();

    @Override
    public <T> T adaptTo(@Nonnull  Object o, @Nonnull Class<T> targetClass) {
        List<AdapterFactory> possibleAdapters = adapterFactories.get(targetClass.getName());
        for ( AdapterFactory af : possibleAdapters) {
            T target = af.adaptTo(o, targetClass);
            if ( target != null) {
                return target;
            }
        }
        return null;
    }



    @Override
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

    @Override
    public synchronized void removeAdapterFactory(AdapterFactory adapterFactory) {
        for( String className: adapterFactory.getTargetClasses()) {
            List<AdapterFactory> possibleAdapters = adapterFactories.get(className);
            if (possibleAdapters != null) {
                possibleAdapters.remove(adapterFactory);
            }
        }

    }
}
