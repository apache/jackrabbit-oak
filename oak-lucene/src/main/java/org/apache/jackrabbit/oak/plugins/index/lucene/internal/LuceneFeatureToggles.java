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
package org.apache.jackrabbit.oak.plugins.index.lucene.internal;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Holder for {@code oak-lucene} feature toggle flags. The {@code .internal} package is not in
 * {@code Export-Package} (see oak-lucene/pom.xml), so toggles here can be removed once retired
 * without an OSGi baseline / API compatibility break.
 */
public final class LuceneFeatureToggles {

    private LuceneFeatureToggles() {
    }

    /**
     * Feature toggle for OAK-12344: ordered numeric and date values that are absent (or could not be converted
     * to the declared type) sort first in ascending and last in descending order, as in the query engine and
     * Elastic (previously they sorted as 0). Enabled by default (bug fix); flipping the toggle sets
     * {@link #FT_OAK_12344_DISABLE} to {@code true} and restores the legacy behavior.
     */
    public static final String FT_OAK_12344 = "FT_OAK-12344";
    public static final AtomicBoolean FT_OAK_12344_DISABLE = new AtomicBoolean(false);
}
