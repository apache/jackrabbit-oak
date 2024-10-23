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

import static java.util.Objects.requireNonNull;

import java.util.Set;

import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

/**
 * Composite observer that delegates all content changes to the set of
 * currently registered component observers.
 */
public class CompositeObserver implements Observer {

    private final Set<Observer> observers = CollectionUtils.newIdentityHashSet();

    public synchronized void addObserver(@NotNull Observer observer) {
        Validate.checkState(observers.add(requireNonNull(observer)));
    }

    public synchronized void removeObserver(@NotNull Observer observer) {
        Validate.checkState(observers.remove(requireNonNull(observer)));
    }

    //----------------------------------------------------------< Observer >--

    @Override
    public synchronized void contentChanged(
            @NotNull NodeState root, @NotNull CommitInfo info) {
        requireNonNull(root);
        for (Observer observer : observers) {
            observer.contentChanged(root, info);
        }
    }

    //------------------------------------------------------------< Object >--

    @Override
    public synchronized String toString() {
        return observers.toString();
    }

}
