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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newIdentityHashSet;

import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Composite observer that delegates all content changes to the set of
 * currently registered component observers.
 */
public class CompositeObserver implements Observer {

    private final Set<Observer> observers = newIdentityHashSet();

    public synchronized void addObserver(@Nonnull Observer observer) {
        checkState(observers.add(checkNotNull(observer)));
    }

    public synchronized void removeObserver(@Nonnull Observer observer) {
        checkState(observers.remove(checkNotNull(observer)));
    }

    //----------------------------------------------------------< Observer >--

    @Override
    public synchronized void contentChanged(
            @Nonnull NodeState root, @Nonnull CommitInfo info) {
        checkNotNull(root);
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
