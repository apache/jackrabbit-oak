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
package org.apache.jackrabbit.oak.spi.xml;

import java.util.Collections;
import javax.annotation.Nonnull;

/**
 * Information about a node being imported. This class is used
 * by the XML import handlers to pass the parsed node information to the
 * import process.
 * <p>
 * An instance of this class is simply a container for the node name,
 * node uuidentifier, and the node type information. See the {@link PropInfo}
 * class for the related carrier of property information.
 */
public class NodeInfo {

    /**
     * Name of the node being imported.
     */
    private final String name;

    /**
     * Name of the primary type of the node being imported.
     */
    private final String primaryTypeName;

    /**
     * Names of the mixin types of the node being imported.
     */
    private final Iterable<String> mixinTypeNames;

    /**
     * UUID of the node being imported.
     */
    private final String uuid;

    /**
     * Creates a node information instance.
     *
     * @param name name of the node being imported
     * @param primaryTypeName name of the primary type of the node being imported
     * @param mixinTypeNames names of the mixin types of the node being imported
     * @param uuid uuid of the node being imported
     */
    public NodeInfo(String name, String primaryTypeName, Iterable<String> mixinTypeNames,
                    String uuid) {
        this.name = name;
        this.primaryTypeName = primaryTypeName;
        this.mixinTypeNames = (mixinTypeNames == null) ? Collections.<String>emptyList() : mixinTypeNames;
        this.uuid = uuid;
    }

    /**
     * Returns the name of the node being imported.
     *
     * @return node name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the name of the primary type of the node being imported.
     *
     * @return primary type name
     */
    public String getPrimaryTypeName() {
        return primaryTypeName;
    }

    /**
     * Returns the names of the mixin types of the node being imported.
     *
     * @return mixin type names
     */
    @Nonnull
    public Iterable<String> getMixinTypeNames() {
        return mixinTypeNames;
    }

    /**
     * Returns the uuid of the node being imported.
     *
     * @return node uuid
     */
    public String getUUID() {
        return uuid;
    }

}