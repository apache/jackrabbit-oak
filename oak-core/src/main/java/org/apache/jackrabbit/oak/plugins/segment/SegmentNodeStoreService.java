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
package org.apache.jackrabbit.oak.plugins.segment;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Dictionary;
import java.util.Map;
import java.util.UUID;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.service.component.ComponentContext;

import com.mongodb.Mongo;

@Component(policy = ConfigurationPolicy.REQUIRE)
@Service(NodeStore.class)
public class SegmentNodeStoreService extends SegmentNodeStore {

    @Property(description="The unique name of this instance")
    public static final String NAME = "name";

    @Property(description="MongoDB host", value="localhost")
    public static final String HOST = "host";

    @Property(description="MongoDB host", intValue=27017)
    public static final String PORT = "port";

    @Property(description="MongoDB database", value="Oak")
    public static final String DB = "db";

    @Property(description="Cache size (MB)", intValue=200)
    public static final String CACHE = "cache";

    private static final long MB = 1024 * 1024;

    private String name;

    private Mongo mongo;

    private final SegmentStore[] store;

    public SegmentNodeStoreService(final SegmentStore[] store) {
        super(new SegmentStore() {
            @Override
            public Journal getJournal(final String name) {
                return new Journal() {
                    @Override
                    public RecordId getHead() {
                        return store[0].getJournal(name).getHead();
                    }
                    @Override
                    public boolean setHead(RecordId base, RecordId head) {
                        return store[0].getJournal(name).setHead(base, head);
                    }
                    @Override
                    public void merge() {
                        store[0].getJournal(name).merge();
                    }
                };
            }
            @Override
            public Segment readSegment(UUID segmentId) {
                return store[0].readSegment(segmentId);
            }            
            @Override
            public void createSegment(
                    UUID segmentId, byte[] bytes, int offset, int length,
                    Collection<UUID> referencedSegmentIds,
                    Map<String, RecordId> strings,
                    Map<Template, RecordId> templates) {
                store[0].createSegment(
                        segmentId, bytes, offset, length,
                        referencedSegmentIds, strings, templates);
            }
            @Override
            public void deleteSegment(UUID segmentId) {
                store[0].deleteSegment(segmentId);
            }
        });
        this.store = store;
    }

    public SegmentNodeStoreService() {
        this(new SegmentStore[1]);
    }

    @Override
    public String toString() {
        return name;
    }

    @Activate
    public void activate(ComponentContext context) throws UnknownHostException {
        Dictionary<?, ?> properties = context.getProperties();
        name = "" + properties.get(NAME);

        String host = String.valueOf(properties.get(HOST));
        int port = Integer.parseInt(String.valueOf(properties.get(PORT)));
        String db = String.valueOf(properties.get(DB));
        int cache = Integer.parseInt(String.valueOf(properties.get(CACHE)));

        mongo = new Mongo(host, port);
        store[0] = new MongoStore(mongo.getDB(db), cache * MB);
    }

    @Deactivate
    public void deactivate() {
        mongo.close();
    }

}
