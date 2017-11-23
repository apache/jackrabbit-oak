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

package org.apache.jackrabbit.oak.plugins.document;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.plugins.document.spi.JournalProperty;
import org.apache.jackrabbit.oak.plugins.document.spi.JournalPropertyBuilder;
import org.apache.jackrabbit.oak.plugins.document.spi.JournalPropertyService;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;

import static com.google.common.base.Preconditions.checkArgument;

class JournalPropertyHandler {
    private final Map<String, JournalPropertyBuilder<JournalProperty>> builders = Maps.newHashMap();

    public JournalPropertyHandler(List<JournalPropertyService> services) {
        for (JournalPropertyService srv : services){
            String name = srv.getName();
            if (!builders.containsKey(name)) {
                builders.put(name, srv.newBuilder());
            } else {
                throw new IllegalStateException("Duplicate JournalPropertyService found " +
                        "for name - " + name + " Currently known services " + services);
            }
        }
    }

    public void readFrom(CommitInfo info){
        CommitContext commitContext = (CommitContext) info.getInfo().get(CommitContext.NAME);

        //Even if commit content is null do a callback to builder to indicate
        //that it may miss out on some data collection
        if (commitContext == null){
            for (JournalPropertyBuilder<?> builder : builders.values()){
                builder.addProperty(null);
            }
            return;
        }

        for (Map.Entry<String,JournalPropertyBuilder<JournalProperty>> e : builders.entrySet()){
            JournalPropertyBuilder<JournalProperty> builder = e.getValue();
            builder.addProperty(getEntry(commitContext, e.getKey()));
        }
    }

    public void readFrom(JournalEntry entry){
        for (Map.Entry<String,JournalPropertyBuilder<JournalProperty>> e : builders.entrySet()){
            JournalPropertyBuilder<JournalProperty> builder = e.getValue();
            String name = Utils.escapePropertyName(e.getKey());
            builder.addSerializedProperty((String) entry.get(name));
        }
    }

    public void addTo(CommitContext commitContext){
        for (Map.Entry<String,JournalPropertyBuilder<JournalProperty>> e : builders.entrySet()){
            JournalPropertyBuilder<JournalProperty> builder = e.getValue();
            commitContext.set(e.getKey(), builder.build());
        }
    }

    public void addTo(UpdateOp op){
        for (Map.Entry<String,JournalPropertyBuilder<JournalProperty>> e : builders.entrySet()){
            JournalPropertyBuilder<JournalProperty> builder = e.getValue();
            String name = Utils.escapePropertyName(e.getKey());
            op.set(name, builder.buildAsString());
        }
    }

    private static JournalProperty getEntry(CommitContext cc, String name){
        Object o = cc.get(name);
        if (o == null){
            return null;
        }
        checkArgument(o instanceof JournalProperty, "CommitContext entry for name [%s] " +
                "having value [%s] is not of type JournalEntry", name, o);
        return (JournalProperty) o;
    }

}
