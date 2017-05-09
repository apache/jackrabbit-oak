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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Basic content change observer that doesn't do anything. Useful as a
 * "null object" for cases where another observer has not been configured,
 * thus avoiding an extra {@code null} check when invoking the observer.
 */
public class EmptyObserver implements Observer {

    /**
     * Static instance of this class, useful as a "null object".
     */
    public static final EmptyObserver INSTANCE = new EmptyObserver();

    @Override
    public void contentChanged(
            @Nonnull NodeState root, @Nonnull CommitInfo info) {
        // do nothing
    }

}
