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

package org.apache.jackrabbit.oak.plugins.blob;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.nanoTime;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.formatTime;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.done;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.newManagementOperation;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.commons.jmx.ManagementOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link BlobGCMBean} based on a {@link BlobGarbageCollector}.
 */
public class  BlobGC extends AnnotatedStandardMBean implements BlobGCMBean {
    private static final Logger log = LoggerFactory.getLogger(BlobGC.class);

    public static final String OP_NAME = "Blob garbage collection";

    private final BlobGarbageCollector blobGarbageCollector;
    private final Executor executor;

    private ManagementOperation<String> gcOp = done(OP_NAME, "");
    
    public static final String CONSISTENCY_OP_NAME = "Blob consistency check";
    
    private ManagementOperation<String> consistencyOp = done(CONSISTENCY_OP_NAME, "");
    
    /**
     * @param blobGarbageCollector  Blob garbage collector
     * @param executor              executor for running the garbage collection task
     */
    public BlobGC(
            @Nonnull BlobGarbageCollector blobGarbageCollector,
            @Nonnull Executor executor) {
        super(BlobGCMBean.class);
        this.blobGarbageCollector = checkNotNull(blobGarbageCollector);
        this.executor = checkNotNull(executor);
    }

    @Nonnull
    @Override
    public CompositeData startBlobGC(final boolean markOnly) {
        if (gcOp.isDone()) {
            gcOp = newManagementOperation(OP_NAME, new Callable<String>() {
                @Override
                public String call() throws Exception {
                    long t0 = nanoTime();
                    blobGarbageCollector.collectGarbage(markOnly);
                    return "Blob gc completed in " + formatTime(nanoTime() - t0);
                }
            });
            executor.execute(gcOp);
        }
        return getBlobGCStatus();
    }

    @Override
    public CompositeData startBlobGC(final boolean markOnly, final boolean forceBlobIdRetrieve) {
        if (gcOp.isDone()) {
            gcOp = newManagementOperation(OP_NAME, new Callable<String>() {
                @Override
                public String call() throws Exception {
                    long t0 = nanoTime();
                    blobGarbageCollector.collectGarbage(markOnly, forceBlobIdRetrieve);
                    return "Blob gc completed in " + formatTime(nanoTime() - t0);
                }
            });
            executor.execute(gcOp);
        }
        return getBlobGCStatus();
    }

    @Nonnull
    @Override
    public CompositeData getBlobGCStatus() {
        return gcOp.getStatus().toCompositeData();
    }
    
    @Override 
    public TabularData getGlobalMarkStats() {
        TabularDataSupport tds;
        try {
            TabularType tt = new TabularType(BlobGC.class.getName(),
                                                "Garbage collection global mark phase Stats", 
                                                TYPE,
                                                new String[] {"repositoryId"});
            tds = new TabularDataSupport(tt);           
            List<GarbageCollectionRepoStats> stats = blobGarbageCollector.getStats();
            for (GarbageCollectionRepoStats stat : stats) {
                tds.put(toCompositeData(stat));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return tds;
    }
    
    @Override 
    public CompositeData checkConsistency() {
        if (consistencyOp.isDone()) {
            consistencyOp = newManagementOperation(CONSISTENCY_OP_NAME, new Callable<String>() {
                @Override
                public String call() throws Exception {
                    long t0 = nanoTime();
                    long missing = blobGarbageCollector.checkConsistency();
                    return "Consistency check completed in "
                                + formatTime(nanoTime() - t0) + ". " +
                                + missing + " missing blobs found (details in the log).";
                }
            });
            executor.execute(consistencyOp);
        }
        return getConsistencyCheckStatus();
    }
    
    @Nonnull
    @Override
    public CompositeData getConsistencyCheckStatus() {
        return consistencyOp.getStatus().toCompositeData();
    }
    
    private CompositeDataSupport toCompositeData(GarbageCollectionRepoStats statObj) throws OpenDataException {
        Object[] values = new Object[] {
                statObj.getRepositoryId() + (statObj.isLocal() ? " *" : ""),
                (statObj.getStartTime() == 0 ? "" : (new Date(statObj.getStartTime()))).toString(),
                (statObj.getEndTime() == 0 ? "" : (new Date(statObj.getEndTime()))).toString(),
                statObj.getLength(),
                humanReadableByteCount(statObj.getLength()),
                statObj.getNumLines()
        };
        return new CompositeDataSupport(TYPE, FIELD_NAMES, values);
    }
    
    private static final String[] FIELD_NAMES = new String[] {
            "repositoryId",
            "markStartTime",
            "markEndTime",
            "referenceFileSizeBytes",
            "referencesFileSize",
            "numReferences",
    };
    
    private static final String[] FIELD_DESCRIPTIONS = new String[] {
           "Repository ID", 
           "Start time of mark",
           "End time of mark",
           "References file size in bytes",
           "References file size in human readable format",
           "Number of references" 
    };
    
    private static final OpenType[] FIELD_TYPES = new OpenType[] {
            SimpleType.STRING,
            SimpleType.STRING,
            SimpleType.STRING,
            SimpleType.LONG,
            SimpleType.STRING,
            SimpleType.INTEGER
    };
    
    private static final CompositeType TYPE = createCompositeType();
    
    private static CompositeType createCompositeType() {
        try {
            return new CompositeType(
                    GarbageCollectionRepoStats.class.getName(),
                    "Composite data type for datastore GC statistics",
                    FIELD_NAMES,
                    FIELD_DESCRIPTIONS,
                    FIELD_TYPES);
        } catch (OpenDataException e) {
            throw new IllegalStateException(e);
        }
    }    
}
