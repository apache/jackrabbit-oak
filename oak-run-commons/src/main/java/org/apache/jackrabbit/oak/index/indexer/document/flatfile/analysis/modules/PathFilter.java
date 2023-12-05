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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules;

import java.util.List;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.StatsCollector;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Storage;

public class PathFilter implements StatsCollector {

    private final StatsCollector base;
    private final String path;
    
    public PathFilter(String path, StatsCollector base) {
        this.path = path;
        this.base = base;
    }
    
    @Override
    public void setStorage(Storage storage) {
        base.setStorage(storage);
    }

    @Override
    public void add(List<String> pathElements, List<Property> properties) {
        for(String pe : pathElements) {
            if (pe.equals(path)) {
                base.add(pathElements, properties);
                break;
            }
        }
    }

    @Override
    public void end() {
        base.end();
    }
    
    public String toString() {
        return "PathFilter " + path + " of " + base.toString();
    }

}
