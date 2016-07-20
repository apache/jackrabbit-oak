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

import static org.apache.jackrabbit.oak.api.Type.STRING;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.StringUtils;

public class TemplateCache extends ReaderCache<Template> {

    /**
     * Create a new template cache.
     *
     * @param maxSize the maximum memory in bytes.
     */
    TemplateCache(long maxSize) {
        super(maxSize, 250, "Template Cache");
    }

    @Override
    protected int getEntryWeight(Template template) {
        int size = 168; // overhead for each cache entry
        size += 40;     // key

        size += estimateMemoryUsage(template.getPrimaryType());
        size += estimateMemoryUsage(template.getMixinTypes());
        size += estimateMemoryUsage(template.getChildName());
        for (PropertyTemplate property : template.getPropertyTemplates()) {
            size += estimateMemoryUsage(property);
        }
        return size;
    }

    private static int estimateMemoryUsage(PropertyTemplate propertyTemplate) {
        return 4 + // index
            estimateMemoryUsage(propertyTemplate.getName());
    }

    private static int estimateMemoryUsage(PropertyState propertyState) {
        if (propertyState == null) {
            return 0;
        }

        int size = estimateMemoryUsage(propertyState.getName());
        for (int k = 0; k < propertyState.count(); k++) {
            size += estimateMemoryUsage(propertyState.getValue(STRING, k));
        }
        return size;
    }

    private static int estimateMemoryUsage(String string) {
        if (string == null) {
            return 0;
        }

        return StringUtils.estimateMemoryUsage(string);
    }

    @Override
    protected boolean isSmall(Template template) {
        PropertyTemplate[] properties = template.getPropertyTemplates();
        PropertyState mixins = template.getMixinTypes();
        return properties.length == 0 && (mixins == null || mixins.count() == 0);
    }

}