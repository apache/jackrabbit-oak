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

package org.apache.jackrabbit.oak.core;

import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;

import static com.google.common.base.Preconditions.checkNotNull;

public class SimpleCommitContext implements CommitContext {
    private final Map<String, Object> attrs = Maps.newHashMap();

    @Override
    public String toString() {
    	return "CommitContext[attrs="+attrs+"]";
    }
    
    @Override
    public void set(String name, Object value) {
        attrs.put(checkNotNull(name), value);
    }

    @Override
    public Object get(String name) {
        return attrs.get(checkNotNull(name));
    }

    @Override
    public void remove(String name) {
        attrs.remove(checkNotNull(name));
    }

    void clear(){
        attrs.clear();
    }
}
