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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Dictionary;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.mongodb.Mongo;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher.Listener;
import org.apache.jackrabbit.oak.plugins.observation.Observable;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.mongo.MongoStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.service.component.ComponentContext;

@Component(policy = ConfigurationPolicy.REQUIRE)
@Service(NodeStore.class)
public class SegmentNodeStoreService implements NodeStore, Observable {

    @Property(description="The unique name of this instance")
    public static final String NAME = "name";

    @Property(description="TarMK directory (if unset, use MongoDB)")
    public static final String DIRECTORY = "repository.home";

    @Property(description="TarMK mode (64 for memory mapping, 32 for normal file access)")
    public static final String MODE = "tarmk.mode";

    @Property(description="TarMK maximum file size")
    public static final String SIZE = "tarmk.size";

    @Property(description="MongoDB host")
    public static final String HOST = "host";

    @Property(description="MongoDB host", intValue=27017)
    public static final String PORT = "port";

    @Property(description="MongoDB database", value="Oak")
    public static final String DB = "db";

    @Property(description="Cache size (MB)", intValue=200)
    public static final String CACHE = "cache";

    private static final int MB = 1024 * 1024;

    private String name;

    private Mongo mongo;

    private SegmentStore store;

    private NodeStore delegate;

    private synchronized NodeStore getDelegate() {
        assert delegate != null : "service must be activated when used";
        return delegate;
    }

    @Activate
    public synchronized void activate(ComponentContext context)
            throws IOException {
        Dictionary<?, ?> properties = context.getProperties();
        name = "" + properties.get(NAME);

        String host = lookup(context, HOST);
        if (host == null) {
            String directory = lookup(context, DIRECTORY);
            if (directory == null) {
                directory = "tarmk";
            }

            String mode = lookup(context, MODE);
            if (mode == null) {
                mode = System.getProperty(MODE,
                        System.getProperty("sun.arch.data.model", "32"));
            }

            String size = lookup(context, SIZE);
            if (size == null) {
                size = System.getProperty(SIZE, "268435456"); // 256MB
            }

            mongo = null;
            store = new FileStore(
                    new File(directory),
                    Integer.parseInt(size), "64".equals(mode));
        } else {
            int port = Integer.parseInt(String.valueOf(properties.get(PORT)));
            String db = String.valueOf(properties.get(DB));
            int cache = Integer.parseInt(String.valueOf(properties.get(CACHE)));

            mongo = new Mongo(host, port);
            store = new MongoStore(mongo.getDB(db), cache * MB);
        }

        delegate = new SegmentNodeStore(store);
    }

    private static String lookup(ComponentContext context, String property) {
        if (context.getProperties().get(property) != null) {
            return context.getProperties().get(property).toString();
        }
        if (context.getBundleContext().getProperty(property) != null) {
            return context.getBundleContext().getProperty(property).toString();
        }
        return null;
    }

    @Deactivate
    public synchronized void deactivate() {
        delegate = null;

        store.close();
        if (mongo != null) {
            mongo.close();
        }
    }

    //------------------------------------------------------------< Observable >---

    @Override
    public Listener newListener() {
        Preconditions.checkState(delegate instanceof Observable);
        return ((Observable) getDelegate()).newListener();
    }


    //---------------------------------------------------------< NodeStore >--

    @Override @Nonnull
    public NodeState getRoot() {
        return getDelegate().getRoot();
    }

    @Nonnull
    @Override
    public NodeState merge(@Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook)
            throws CommitFailedException {
        return getDelegate().merge(builder, commitHook);
    }

    @Override
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        return getDelegate().rebase(builder);
    }

    @Override
    public NodeState reset(@Nonnull NodeBuilder builder) {
        return getDelegate().reset(builder);
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return getDelegate().createBlob(stream);
    }

    @Override @Nonnull
    public String checkpoint(long lifetime) {
        return getDelegate().checkpoint(lifetime);
    }

    @Override @CheckForNull
    public NodeState retrieve(@Nonnull String checkpoint) {
        return getDelegate().retrieve(checkpoint);
    }

    //------------------------------------------------------------< Object >--

    @Override
    public synchronized String toString() {
        return name + ": " + super.toString();
    }

}
