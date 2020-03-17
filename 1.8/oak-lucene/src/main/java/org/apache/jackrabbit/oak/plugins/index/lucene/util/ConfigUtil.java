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

package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import java.util.Collections;

import com.google.common.primitives.Ints;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkArgument;

public class ConfigUtil {

    public static boolean getOptionalValue(NodeState definition, String propName, boolean defaultVal){
        PropertyState ps = definition.getProperty(propName);
        return ps == null ? defaultVal : ps.getValue(Type.BOOLEAN);
    }

    public static int getOptionalValue(NodeState definition, String propName, int defaultVal){
        PropertyState ps = definition.getProperty(propName);
        return ps == null ? defaultVal : Ints.checkedCast(ps.getValue(Type.LONG));
    }

    public static String getOptionalValue(NodeState definition, String propName, String defaultVal){
        PropertyState ps = definition.getProperty(propName);
        return ps == null ? defaultVal : ps.getValue(Type.STRING);
    }

    public static float getOptionalValue(NodeState definition, String propName, float defaultVal){
        PropertyState ps = definition.getProperty(propName);
        return ps == null ? defaultVal : ps.getValue(Type.DOUBLE).floatValue();
    }

    public static double getOptionalValue(NodeState definition, String propName, double defaultVal){
        PropertyState ps = definition.getProperty(propName);
        return ps == null ? defaultVal : ps.getValue(Type.DOUBLE);
    }

    public static String getPrimaryTypeName(NodeState nodeState) {
        PropertyState ps = nodeState.getProperty(JcrConstants.JCR_PRIMARYTYPE);
        return (ps == null) ? JcrConstants.NT_BASE : ps.getValue(Type.NAME);
    }

    public static Iterable<String> getMixinNames(NodeState nodeState) {
        PropertyState ps = nodeState.getProperty(JcrConstants.JCR_MIXINTYPES);
        return (ps == null) ? Collections.<String>emptyList() : ps.getValue(Type.NAMES);
    }

    /**
     * Assumes that given state is of type nt:file and then reads
     * the jcr:content/@jcr:data property to get the binary content
     */
    public static Blob getBlob(NodeState state, String resourceName){
        NodeState contentNode = state.getChildNode(JcrConstants.JCR_CONTENT);
        checkArgument(contentNode.exists(), "Was expecting to find jcr:content node to read resource %s", resourceName);
        return contentNode.getProperty(JcrConstants.JCR_DATA).getValue(Type.BINARY);
    }
}
