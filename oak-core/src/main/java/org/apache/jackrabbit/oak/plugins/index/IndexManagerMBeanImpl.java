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
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.commons.jdkcompat.Java23Subject;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAuditTrail.Entry;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.jetbrains.annotations.Nullable;

/**
 * JMX access to the {@link IndexManager} (disable, enable and delete index definitions) and
 * to the audit trail written by {@link IndexDefinitionAudit}.
 *
 * @see <a href="https://issues.apache.org/jira/browse/OAK-12458">OAK-12458</a>
 */
public class IndexManagerMBeanImpl extends AnnotatedStandardMBean implements IndexManagerMBean {

    static final String UNKNOWN_CALLER = "unknown";

    /*
     * JMX clients show the items of a row sorted by name, so the names are numbered to
     * keep them in this order.
     */
    private static final String[] INDEX_ITEMS = {
            "1. index", "2. lastOperation", "3. lastTime", "4. entries", "5. lastUser"};

    private static final OpenType<?>[] INDEX_TYPES = {
            SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.INTEGER, SimpleType.STRING};

    private static final String[] ENTRY_ITEMS = {
            "1. operation", "2. time", "3. user", "4. reason", "5. thread", "6. diff"};

    private static final OpenType<?>[] ENTRY_TYPES = {
            SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING,
            SimpleType.STRING};

    private final NodeStore store;

    private final IndexManager manager;

    /**
     * @param store        the node store
     * @param indexEditors the index editors of the repository
     * @param feature      the feature toggle that enables the index definition audit; changes
     *                     are only possible if the audit is enabled (also with the system
     *                     property {@link IndexDefinitionAudit#ENABLED_PROPERTY})
     */
    public IndexManagerMBeanImpl(NodeStore store, IndexEditorProvider indexEditors, @Nullable Feature feature) {
        super(IndexManagerMBean.class);
        this.store = store;
        this.manager = new IndexManager(store, indexEditors, feature);
    }

    @Override
    public TabularData getIndexes() throws OpenDataException {
        CompositeType rowType = new CompositeType("IndexDefinitionAuditIndex", "Audited index",
                INDEX_ITEMS, INDEX_ITEMS, INDEX_TYPES);
        TabularDataSupport result = new TabularDataSupport(
                new TabularType("IndexDefinitionAuditIndexes", "Audited indexes", rowType, new String[] {INDEX_ITEMS[0]}));
        for (Map.Entry<String, List<Entry>> e : new IndexDefinitionAuditTrail(store.getRoot()).getEntriesByIndex().entrySet()) {
            List<Entry> entries = e.getValue();
            Entry last = entries.get(entries.size() - 1);
            result.put(new CompositeDataSupport(rowType, INDEX_ITEMS, new Object[] {
                    e.getKey(), last.operation(), last.time(), entries.size(), last.user()}));
        }
        return result;
    }

    @Override
    public TabularData getAuditTrail(String indexPath) throws OpenDataException {
        CompositeType rowType = new CompositeType("IndexDefinitionAuditEntry", "Audit entry",
                ENTRY_ITEMS, ENTRY_ITEMS, ENTRY_TYPES);
        TabularDataSupport result = new TabularDataSupport(
                new TabularType("IndexDefinitionAuditEntries", "Audit entries", rowType, ENTRY_ITEMS));
        for (Entry entry : new IndexDefinitionAuditTrail(store.getRoot()).getEntries(toIndexPath(indexPath))) {
            CompositeDataSupport row = new CompositeDataSupport(rowType, ENTRY_ITEMS, new Object[] {
                    entry.operation(), entry.time(), entry.user(), entry.reason(), entry.thread(), entry.diff()});
            // the rows have no key column: identical rows (same change at the same time) are shown once
            if (!result.containsKey(result.calculateIndex(row))) {
                result.put(row);
            }
        }
        return result;
    }

    @Override
    public String disableIndex(String indexPath, String by, String reason, boolean force) {
        return manager.disable(toIndexPath(indexPath), toCaller(by), reason, force);
    }

    @Override
    public String enableIndex(String indexPath, String by, boolean force) {
        return manager.enable(toIndexPath(indexPath), toCaller(by), force);
    }

    @Override
    public String deleteIndex(String indexPath, String by, String reason, boolean force) {
        return manager.delete(toIndexPath(indexPath), toCaller(by), reason, force);
    }

    /**
     * Returns the path of an index given by name (relative to {@code /oak:index}) or by path.
     */
    private static String toIndexPath(String indexPath) {
        String path = indexPath == null ? "" : indexPath.trim();
        // not validated here: the index manager rejects invalid paths
        return path.startsWith("/") ? path : "/" + INDEX_DEFINITIONS_NAME + "/" + path;
    }

    /**
     * Returns who changes the index definition: the given value, else the principals of the
     * JMX caller (if known), else {@link #UNKNOWN_CALLER}.
     */
    private static String toCaller(String by) {
        if (by != null && !by.isBlank()) {
            return by.trim();
        }
        Subject subject = Java23Subject.getSubject();
        if (subject != null && !subject.getPrincipals().isEmpty()) {
            return subject.getPrincipals().stream().map(Principal::getName).sorted()
                    .collect(Collectors.joining(","));
        }
        return UNKNOWN_CALLER;
    }
}
