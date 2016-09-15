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

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Policy which performs immediate refresh upon completion of writes
 */
public class RefreshOnWritePolicy implements ReaderRefreshPolicy, IndexUpdateListener {
    private final AtomicBoolean dirty = new AtomicBoolean();

    @Override
    public void refreshOnReadIfRequired(Runnable refreshCallback) {
        //As writer itself refreshes the index. No refresh done
        //on read
    }

    @Override
    public void refreshOnWriteIfRequired(Runnable refreshCallback) {
        //For sync indexing mode we refresh the reader immediately
        //on the writer thread. So that any read call later sees upto date index
        if (dirty.get()) {
            refreshCallback.run();
            dirty.set(false);
        }
    }

    @Override
    public void updated() {
        dirty.set(true);
    }
}
