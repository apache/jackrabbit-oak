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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;

import java.util.Formatter;
import java.util.Set;

import javax.annotation.Nonnull;

/**
 * This utility breaks down space usage per record type.
 * It accounts for value sharing. That is, an instance
 * of this class will remember which records it has seen
 * already and not count those again. Only the effective
 * space taken by the records is taken into account. Slack
 * space from aligning records is not accounted for.
 */
public class RecordUsageAnalyser extends SegmentParser {
    private final RecordIdSet seenIds = new RecordIdSet();
    private final Set<String> deadLinks = newHashSet();

    private long mapSize;       // leaf and branch
    private long listSize;      // list and bucket
    private long valueSize;     // inlined values
    private long templateSize;  // template
    private long nodeSize;      // node

    private long mapCount;
    private long listCount;
    private long propertyCount;
    private long smallBlobCount;
    private long mediumBlobCount;
    private long longBlobCount;
    private long externalBlobCount;
    private long smallStringCount;
    private long mediumStringCount;
    private long longStringCount;
    private long templateCount;
    private long nodeCount;

    public RecordUsageAnalyser(@Nonnull SegmentReader reader) {
        super(reader);
    }

    /**
     * @return number of bytes in {@link RecordType#LEAF leaf} and {@link RecordType#BRANCH branch}
     * records.
     */
    public long getMapSize() {
        return mapSize;
    }

    /**
     * @return number of bytes in {@link RecordType#LIST list} and {@link RecordType#BUCKET bucket}
     * records.
     */
    public long getListSize() {
        return listSize;
    }

    /**
     * @return number of bytes in inlined values (strings and blobs)
     */
    public long getValueSize() {
        return valueSize;
    }

    /**
     * @return number of bytes in {@link RecordType#TEMPLATE template} records.
     */
    public long getTemplateSize() {
        return templateSize;
    }

    /**
     * @return number of bytes in {@link RecordType#NODE node} records.
     */
    public long getNodeSize() {
        return nodeSize;
    }

    /**
     * @return number of maps
     */
    public long getMapCount() {
        return mapCount;
    }

    /**
     * @return number of lists
     */
    public long getListCount() {
        return listCount;
    }

    /**
     * @return number of properties
     */
    public long getPropertyCount() {
        return propertyCount;
    }

    /**
     * @return number of {@link Segment#SMALL_LIMIT small} blobs.
     *
     */
    public long getSmallBlobCount() {
        return smallBlobCount;
    }

    /**
     * @return number of {@link Segment#MEDIUM_LIMIT medium} blobs.
     *
     */
    public long getMediumBlobCount() {
        return mediumBlobCount;
    }

    /**
     * @return number of long blobs.
     *
     */
    public long getLongBlobCount() {
        return longBlobCount;
    }

    /**
     * @return number of external blobs.
     *
     */
    public long getExternalBlobCount() {
        return externalBlobCount;
    }

    /**
     * @return number of {@link Segment#SMALL_LIMIT small} strings.
     *
     */
    public long getSmallStringCount() {
        return smallStringCount;
    }

    /**
     * @return number of {@link Segment#MEDIUM_LIMIT medium} strings.
     *
     */
    public long getMediumStringCount() {
        return mediumStringCount;
    }

    /**
     * @return number of long strings.
     *
     */
    public long getLongStringCount() {
        return longStringCount;
    }

    /**
     * @return number of templates.
     */
    public long getTemplateCount() {
        return templateCount;
    }

    /**
     * @return number of nodes.
     */
    public long getNodeCount() {
        return nodeCount;
    }

    public void analyseNode(RecordId nodeId) {
        onNode(null, nodeId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        @SuppressWarnings("resource")
        Formatter formatter = new Formatter(sb);
        formatter.format(
                "%s in maps (%s leaf and branch records)%n",
                byteCountToDisplaySize(mapSize), mapCount);
        formatter.format(
                "%s in lists (%s list and bucket records)%n",
                byteCountToDisplaySize(listSize), listCount);
        formatter.format(
                "%s in values (value and block records of %s properties, " +
                "%s/%s/%s/%s small/medium/long/external blobs, %s/%s/%s small/medium/long strings)%n",
                byteCountToDisplaySize(valueSize), propertyCount,
                smallBlobCount, mediumBlobCount, longBlobCount, externalBlobCount,
                smallStringCount, mediumStringCount, longStringCount);
        formatter.format(
                "%s in templates (%s template records)%n",
                byteCountToDisplaySize(templateSize), templateCount);
        formatter.format(
                "%s in nodes (%s node records)%n",
                byteCountToDisplaySize(nodeSize), nodeCount);
        formatter.format("links to non existing segments: %s", deadLinks);
        return sb.toString();
    }

    @Override
    protected void onNode(RecordId parentId, RecordId nodeId) {
        try {
            if (seenIds.addIfNotPresent(nodeId)) {
                NodeInfo info = parseNode(nodeId);
                this.nodeCount++;
                this.nodeSize += info.size;
            }
        } catch (SegmentNotFoundException snfe) {
            deadLinks.add(snfe.getSegmentId());
        }
    }

    @Override
    protected void onTemplate(RecordId parentId, RecordId templateId) {
        try {
            if (seenIds.addIfNotPresent(templateId)) {
                TemplateInfo info = parseTemplate(templateId);
                this.templateCount++;
                this.templateSize += info.size;
            }
        } catch (SegmentNotFoundException snfe) {
            deadLinks.add(snfe.getSegmentId());
        }
    }

    @Override
    protected void onMapDiff(RecordId parentId, RecordId mapId, MapRecord map) {
        try {
            if (seenIds.addIfNotPresent(mapId)) {
                MapInfo info = parseMapDiff(mapId, map);
                this.mapCount++;
                this.mapSize += info.size;
            }
        } catch (SegmentNotFoundException snfe) {
            deadLinks.add(snfe.getSegmentId());
        }
    }

    @Override
    protected void onMapLeaf(RecordId parentId, RecordId mapId, MapRecord map) {
        try {
            if (seenIds.addIfNotPresent(mapId)) {
                MapInfo info = parseMapLeaf(mapId, map);
                this.mapCount++;
                this.mapSize += info.size;
            }
        } catch (SegmentNotFoundException snfe) {
            deadLinks.add(snfe.getSegmentId());
        }
    }

    @Override
    protected void onMapBranch(RecordId parentId, RecordId mapId, MapRecord map) {
        try {
            if (seenIds.addIfNotPresent(mapId)) {
                MapInfo info = parseMapBranch(mapId, map);
                this.mapCount++;
                this.mapSize += info.size;
            }
        } catch (SegmentNotFoundException snfe) {
            deadLinks.add(snfe.getSegmentId());
        }
    }

    @Override
    protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) {
        try {
            if (!seenIds.contains(propertyId)) {
                PropertyInfo info = parseProperty(parentId, propertyId, template);
                this.propertyCount++;
                this.valueSize += info.size;
                seenIds.addIfNotPresent(propertyId);
            }
        } catch (SegmentNotFoundException snfe) {
            deadLinks.add(snfe.getSegmentId());
        }
    }

    @Override
    protected void onBlob(RecordId parentId, RecordId blobId) {
        try {
            if (seenIds.addIfNotPresent(blobId)) {
                BlobInfo info = parseBlob(blobId);
                this.valueSize += info.size;
                switch (info.blobType) {
                    case SMALL:
                        this.smallBlobCount++;
                        break;
                    case MEDIUM:
                        this.mediumBlobCount++;
                        break;
                    case LONG:
                        this.longBlobCount++;
                        break;
                    case EXTERNAL:
                        this.externalBlobCount++;
                        break;
                }
            }
        } catch (SegmentNotFoundException snfe) {
            deadLinks.add(snfe.getSegmentId());
        }
    }

    @Override
    protected void onString(RecordId parentId, RecordId stringId) {
        try {
            if (seenIds.addIfNotPresent(stringId)) {
                BlobInfo info = parseString(stringId);
                this.valueSize += info.size;
                switch (info.blobType) {
                    case SMALL:
                        this.smallStringCount++;
                        break;
                    case MEDIUM:
                        this.mediumStringCount++;
                        break;
                    case LONG:
                        this.longStringCount++;
                        break;
                    case EXTERNAL:
                        throw new IllegalStateException("String is too long: " + info.size);
                }
            }
        } catch (SegmentNotFoundException snfe) {
            deadLinks.add(snfe.getSegmentId());
        }
    }

    @Override
    protected void onList(RecordId parentId, RecordId listId, int count) {
        try {
            if (seenIds.addIfNotPresent(listId)) {
                ListInfo info = parseList(parentId, listId, count);
                this.listCount++;
                this.listSize += info.size;
            }
        } catch (SegmentNotFoundException snfe) {
            deadLinks.add(snfe.getSegmentId());
        }
    }

}
