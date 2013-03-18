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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Validator that does nothing by default and doesn't recurse into subtrees.
 * Useful as a sentinel or as a base class for more complex validators.
 *
 * @since Oak 0.3
 */
public class DefaultValidator implements Validator {

    public static final Validator INSTANCE = new DefaultValidator();

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        // do nothing
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        // do nothing
    }

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        // do nothing
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        // do nothing
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        // do nothing
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        return null;
    }

    @Override
    public Validator childNodeChanged(
            String name, NodeState before, NodeState after)
            throws CommitFailedException {
        return null;
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        return null;
    }

}
