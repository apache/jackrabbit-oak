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

package org.apache.jackrabbit.oak.plugins.index;

import java.io.File;
import java.io.IOException;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.jmx.IndexerMBean;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.index.importer.AbortingIndexerLock;
import org.apache.jackrabbit.oak.plugins.index.importer.AsyncIndexerLock;
import org.apache.jackrabbit.oak.plugins.index.importer.ClusterNodeStoreLock;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporter;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporterProvider;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@Component
public class IndexerMBeanImpl extends AnnotatedStandardMBean implements IndexerMBean {
    private final Logger log = LoggerFactory.getLogger(getClass());
    @Reference
    private NodeStore nodeStore;

    @Reference
    private AsyncIndexInfoService asyncIndexInfoService;

    private WhiteboardIndexEditorProvider editorProvider = new WhiteboardIndexEditorProvider();
    private Registration mbeanReg;
    private Tracker<IndexImporterProvider> providerTracker;

    public IndexerMBeanImpl() {
        super(IndexerMBean.class);
    }

    @Override
    public boolean importIndex(String indexDirPath) throws IOException, CommitFailedException {
        try {
            IndexImporter importer = new IndexImporter(nodeStore, new File(indexDirPath), editorProvider, createLock());
            providerTracker.getServices().forEach(importer::addImporterProvider);
            importer.importIndex();
        } catch (IOException | CommitFailedException | RuntimeException e) {
            log.warn("Error occurred while importing index from path [{}]", indexDirPath, e);
            throw e;
        }
        return true;
    }

    private AsyncIndexerLock createLock() {
        if (nodeStore instanceof Clusterable) {
            return new ClusterNodeStoreLock(nodeStore);
        }
        return new AbortingIndexerLock(asyncIndexInfoService);
    }


    //~---------------------------------------< OSGi >

    @Activate
    private void activate(BundleContext context) {
        Whiteboard wb = new OsgiWhiteboard(context);
        editorProvider.start(wb);
        mbeanReg = registerMBean(wb,
                IndexerMBean.class,
                this,
                IndexerMBean.TYPE,
                "Indexer operations related MBean");
        providerTracker = wb.track(IndexImporterProvider.class);
    }

    @Deactivate
    private void deactivate() {
        if (mbeanReg != null) {
            mbeanReg.unregister();
        }
        editorProvider.stop();
        if (providerTracker != null) {
            providerTracker.stop();
        }
    }

}
