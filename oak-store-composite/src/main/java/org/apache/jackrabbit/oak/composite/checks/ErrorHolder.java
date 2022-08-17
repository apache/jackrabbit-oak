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
package org.apache.jackrabbit.oak.composite.checks;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.IllegalRepositoryStateException;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ErrorHolder {
    
    private static final int FAIL_IMMEDIATELY_THRESHOLD = 100;
    private final List<String> errors = new ArrayList<>();
    
    private static final Logger log = LoggerFactory.getLogger(ErrorHolder.class);

    public void report(MountedNodeStore mountedStore, String path, String error, MountedNodeStoreChecker<?> checker) {
        String msg = String.format("For NodeStore mount %s, path %s, encountered the following problem: '%s' (via checker %s)", mountedStore.getMount().getName(), path, error, checker);
        log.error(msg);
        errors.add(msg);
        if ( errors.size() == FAIL_IMMEDIATELY_THRESHOLD ) { 
            end();
        }
    }
    
    public void report(MountedNodeStore firstNS, String firstPath, MountedNodeStore secondNS, String secondPath, String value, String error, MountedNodeStoreChecker<?> checker) {
        String msg = String.format("For NodeStore mount %s, path %s, and NodeStore mount %s, path %s, encountered the following clash for value %s: '%s' (via checker %s)", 
                firstNS.getMount().getName(), firstPath, secondNS.getMount().getName(), secondPath, value, error, checker);
        log.error(msg);
        errors.add(msg);
        if ( errors.size() == FAIL_IMMEDIATELY_THRESHOLD ) { 
            end();
        }
    }
    
    public void end() {
        if ( errors.isEmpty() ) {
            return;
        }
        StringBuilder out = new StringBuilder();
        out.append(errors.size()).append(" errors were found: \n");
        errors.forEach( e -> out.append(e).append('\n'));
        
        throw new IllegalRepositoryStateException(out.toString());
    }
}