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
package org.apache.jackrabbit.mk.cluster;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.fast.Jsop;
import org.apache.jackrabbit.mk.json.fast.JsopArray;
import org.apache.jackrabbit.mk.json.fast.JsopObject;
import org.apache.jackrabbit.mk.util.PathUtils;

/**
 * This class connects two <code>MicroKernel</code> instances, where one
 * instance will periodically commit the changes made to the other.
 * 
 * TODO do a full sync on first call
 * TODO add periodic background check
 */
public class HotBackup {

    private static final String PATH_PROPERTY_LASTREV = "/:lastrev"; 
    private final MicroKernel source;
    private final MicroKernel target;
    private String lastRev;

    /**
     * Create a new instance of this class.
     * 
     * @param source source microkernel where changes are read
     * @param target target microkernel where changes are committed
     */
    public HotBackup(MicroKernel source, MicroKernel target) {
        this.source = source;
        this.target = target;
        
        init();
    }
    
    private void init() {
        lastRev = getProperty(target, PATH_PROPERTY_LASTREV);
        if (lastRev == null) {
            lastRev = source.getHeadRevision();
            
            // TODO never sync'ed, so do a full copy
        
            
            setProperty(target, PATH_PROPERTY_LASTREV, lastRev);
        }
        sync();
    }

    /**
     * Read all changes from the source microkernel and commit them to
     * the target microkernel.
     */
    public void sync() {
        String headRev = source.getHeadRevision();
        if (lastRev != headRev) {
            JsopArray journal = (JsopArray) Jsop.parse(source.getJournal(lastRev, headRev, null));
            for (int i = 0; i < journal.size(); i++) {
                JsopObject record = (JsopObject) journal.get(i);
                String diff = (String) record.get("changes");
                String message = (String) record.get("msg");
                target.commit("", diff, target.getHeadRevision(), message);
            }
            lastRev = headRev;
            setProperty(target, PATH_PROPERTY_LASTREV, lastRev);
        }
    }
    
    private static String getProperty(MicroKernel mk, String path) {
        String parent = PathUtils.getParentPath(path);
        String name = PathUtils.getName(path);
        
        // todo use filter parameter for specifying the property?
        JsopObject props = (JsopObject) Jsop.parse(mk.getNodes(parent, mk.getHeadRevision(), -1, 0, -1, null));
        return (String) props.get(name);
    }

    private static void setProperty(MicroKernel mk, String path, String value) {
        String parent = PathUtils.getParentPath(path);
        String name = PathUtils.getName(path); 
        
        if (value == null) {
            String diff = new JsopBuilder().tag('-').key(name).value(null).toString();
            mk.commit(parent, diff, mk.getHeadRevision(), null);
        } else {
            String diff = new JsopBuilder().tag('+').key(name).value(value).toString();
            mk.commit(parent, diff, mk.getHeadRevision(), null);
        }
    }
}
