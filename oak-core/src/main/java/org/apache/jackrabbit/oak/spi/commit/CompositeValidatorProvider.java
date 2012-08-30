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
package org.apache.jackrabbit.oak.spi.commit;

import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * This {@code ValidatorProvider} aggregates a list of validator providers into
 * a single validator provider.
 */
public class CompositeValidatorProvider implements ValidatorProvider {
    private final Collection<ValidatorProvider> providers;

    public CompositeValidatorProvider(Collection<ValidatorProvider> providers) {
        this.providers = providers;
    }

    public CompositeValidatorProvider(ValidatorProvider... providers) {
        this(Arrays.asList(providers));
    }

    @Override
    public Validator getRootValidator(NodeState before, NodeState after) {
        List<Validator> rootValidators = new ArrayList<Validator>(providers.size());

        for (ValidatorProvider provider : providers) {
            rootValidators.add(provider.getRootValidator(before, after));
        }

        return new CompositeValidator(rootValidators);
    }

}
