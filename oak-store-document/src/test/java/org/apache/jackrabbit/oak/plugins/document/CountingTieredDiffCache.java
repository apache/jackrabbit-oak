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
package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class CountingTieredDiffCache extends TieredDiffCache {

    class CountingLoader implements Loader {

        private Loader delegate;

        CountingLoader(Loader delegate) {
            this.delegate = delegate;
        }

        @Override
        public String call() {
            incLoadCount();
            return delegate.call();
        }

    }

    private int loadCount;

    public CountingTieredDiffCache(DocumentMK.Builder builder) {
        super(builder);
    }

    private void incLoadCount() {
        loadCount++;
    }

    public int getLoadCount() {
        return loadCount;
    }

    public void resetLoadCounter() {
        loadCount = 0;
    }

    @Override
    public String getChanges(@Nonnull RevisionVector from,
                             @Nonnull RevisionVector to,
                             @Nonnull String path,
                             @Nullable Loader loader) {
        return super.getChanges(from, to, path, new CountingLoader(loader));
    }
}
