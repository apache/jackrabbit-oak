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

package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.segment.CacheWeights.ReaderTemplateCacheWeigher;

public class TemplateCache extends ReaderCache<Template> {

    /**
     * Create a new template cache.
     *
     * @param maxSize the maximum memory in bytes.
     */
    TemplateCache(long maxSize) {
        super(maxSize, 250, "Template Cache", new ReaderTemplateCacheWeigher());
    }

    @Override
    protected boolean isSmall(Template template) {
        PropertyTemplate[] properties = template.getPropertyTemplates();
        PropertyState mixins = template.getMixinTypes();
        return properties.length == 0 && (mixins == null || mixins.count() == 0);
    }

}