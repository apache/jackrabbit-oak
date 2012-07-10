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
package org.apache.jackrabbit.oak.jcr.nodetype;

import java.util.List;

public class PropertyDefinitionDelegate extends ItemDefinitionDelegate {

    private final int requiredType;
    private final boolean multiple;
    private final List<String> defaultValues;

    protected PropertyDefinitionDelegate(String name, boolean autoCreated, boolean mandatory, int onParentVersion,
            boolean isProtected, int requiredType, boolean multiple, List<String> defaultValues) {
        super(name, autoCreated, mandatory, onParentVersion, isProtected);
        this.requiredType = requiredType;
        this.multiple = multiple;
        this.defaultValues = defaultValues;
    }

    public List<String> getDefaultValues() {
        return defaultValues;
    }

    public int getRequiredType() {
        return requiredType;
    }

    public boolean isMultiple() {
        return multiple;
    }
}
