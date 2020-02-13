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
package org.apache.jackrabbit.oak.plugins.index.lucene.score.impl;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.lucene.score.ScorerProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.score.ScorerProviderFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component(metatype = false, immediate = true)
@Service(value = ScorerProviderFactory.class)
@Reference(name = "ScorerProvider",
        policy = ReferencePolicy.DYNAMIC,
        cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
        referenceInterface = ScorerProvider.class,
        bind = "bindScorerProvider",
        unbind = "unbindScorerProvider"
)
public class ScorerProviderFactoryImpl implements ScorerProviderFactory {

    private Map<String, ScorerProvider> scorerProviderMap =
            new ConcurrentHashMap<String, ScorerProvider>();

    @Deactivate
    private void deactivate() {
        scorerProviderMap.clear();
    }

    @Override
    public ScorerProvider getScorerProvider(String scorerName) {
        return scorerProviderMap.get(scorerName);
    }

    private void bindScorerProvider(ScorerProvider provider) {
        scorerProviderMap.put(provider.getName(), provider);
    }

    private void unbindScorerProvider(ScorerProvider provider) {
        scorerProviderMap.remove(provider.getName());
    }
}
