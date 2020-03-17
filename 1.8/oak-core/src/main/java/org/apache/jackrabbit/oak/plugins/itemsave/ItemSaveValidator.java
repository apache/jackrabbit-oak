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
package org.apache.jackrabbit.oak.plugins.itemsave;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

import java.util.List;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.FailingValidator;
import org.apache.jackrabbit.oak.spi.commit.SubtreeExcludingValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;

/**
 * This validator checks that all changes are contained within the subtree
 * rooted at a given path.
 */
class ItemSaveValidator extends SubtreeExcludingValidator {

    /**
     * Name of the property whose {@link #propertyChanged(org.apache.jackrabbit.oak.api.PropertyState, org.apache.jackrabbit.oak.api.PropertyState)} to
     * ignore or {@code null} if no property should be ignored.
     */
    private final String ignorePropertyChange;

    /**
     * Create a new validator that only throws a {@link CommitFailedException} whenever
     * there are changes not contained in the subtree rooted at {@code path}.
     * @param path
     */
    public ItemSaveValidator(String path) {
        this(new FailingValidator(CommitFailedException.UNSUPPORTED, 0,
                "Failed to save subtree at " + path + ". There are " +
                        "transient modifications outside that subtree."),
                newArrayList(elements(path)));
    }

    private ItemSaveValidator(Validator validator, List<String> path) {
        super(validator, path);
        // Ignore property changes if this is the head of the path.
        // This allows for calling save on a changed property.
        ignorePropertyChange = path.size() == 1 ? path.get(0) : null;
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        if (!before.getName().equals(ignorePropertyChange)) {
            super.propertyChanged(before, after);
        }
    }

    @Override
    protected SubtreeExcludingValidator createValidator(
            Validator validator, final List<String> path) {
        return new ItemSaveValidator(validator, path);
    }

}
