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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Dictionary;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.service.component.ComponentContext;

@Component(policy = ConfigurationPolicy.REQUIRE)
@Service(NodeStore.class)
public class SegmentNodeStoreService implements NodeStore, Observable {

    @Property(description="The unique name of this instance")
    public static final String NAME = "name";

    @Property(description="TarMK directory")
    public static final String DIRECTORY = "repository.home";

    @Property(description="TarMK mode (64 for memory mapping, 32 for normal file access)", value="64")
    public static final String MODE = "tarmk.mode";

    @Property(description="TarMK maximum file size (MB)", intValue=256)
    public static final String SIZE = "tarmk.size";

    @Property(description="Cache size (MB)", intValue=256)
    public static final String CACHE = "cache";

    private String name;

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
            size = System.getProperty(SIZE, "256");
        }

        store = new FileStore(
                new File(directory),
                Integer.parseInt(size), "64".equals(mode));

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
        store = null;
    }

    //------------------------------------------------------------< Observable >---

    @Override
    public Closeable addObserver(Observer observer) {
        Preconditions.checkState(delegate instanceof Observable);
        return ((Observable) getDelegate()).addObserver(observer);
    }


    //---------------------------------------------------------< NodeStore >--

    @Override @Nonnull
    public NodeState getRoot() {
        return getDelegate().getRoot();
    }

    @Nonnull
    @Override
    public NodeState merge(
            @Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook,
            @Nullable CommitInfo info) throws CommitFailedException {
        return getDelegate().merge(builder, commitHook, info);
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
