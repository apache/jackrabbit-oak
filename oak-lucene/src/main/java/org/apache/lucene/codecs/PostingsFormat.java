/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.codecs;

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

import java.io.IOException;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.NamedSPILoader;

/**
 * Encodes/decodes terms, postings, and proximity data.
 * <p>
 * Note, when extending this class, the name ({@link #getName}) may written into the index in
 * certain configurations. In order for the segment to be read, the name must resolve to your
 * implementation via {@link #forName(String)}. This method uses Java's
 * {@link ServiceLoader Service Provider Interface} (SPI) to resolve format names.
 * <p>
 * If you implement your own format, make sure that it has a no-arg constructor so SPI can load it.
 *
 * @lucene.experimental
 * @see ServiceLoader
 */
public abstract class PostingsFormat implements NamedSPILoader.NamedSPI {

    private static final NamedSPILoader<PostingsFormat> loader =
        new NamedSPILoader<PostingsFormat>(PostingsFormat.class);

    /**
     * Zero-length {@code PostingsFormat} array.
     */
    public static final PostingsFormat[] EMPTY = new PostingsFormat[0];

    /**
     * Unique name that's used to retrieve this format when reading the index.
     */
    private final String name;

    /**
     * Creates a new postings format.
     * <p>
     * The provided name will be written into the index segment in some configurations (such as when
     * using {@link PerFieldPostingsFormat}): in such configurations, for the segment to be read
     * this class should be registered with Java's SPI mechanism (registered in META-INF/ of your
     * jar file, etc).
     *
     * @param name must be all ascii alphanumeric, and less than 128 characters in length.
     */
    protected PostingsFormat(String name) {
        NamedSPILoader.checkServiceName(name);
        this.name = name;
    }

    /**
     * Returns this posting format's name
     */
    @Override
    public final String getName() {
        return name;
    }

    /**
     * Writes a new segment
     */
    public abstract FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException;

    /**
     * Reads a segment.  NOTE: by the time this call returns, it must hold open any files it will
     * need to use; else, those files may be deleted. Additionally, required files may be deleted
     * during the execution of this call before there is a chance to open them. Under these
     * circumstances an IOException should be thrown by the implementation. IOExceptions are
     * expected and will automatically cause a retry of the segment opening logic with the newly
     * revised segments.
     */
    public abstract FieldsProducer fieldsProducer(SegmentReadState state) throws IOException;

    @Override
    public String toString() {
        return "PostingsFormat(name=" + name + ")";
    }

    /**
     * looks up a format by name
     */
    public static PostingsFormat forName(String name) {
        if (loader == null) {
            throw new IllegalStateException(
                "You called PostingsFormat.forName() before all formats could be initialized. " +
                    "This likely happens if you call it from a PostingsFormat's ctor.");
        }
        return loader.lookup(name);
    }

    /**
     * returns a list of all available format names
     */
    public static Set<String> availablePostingsFormats() {
        if (loader == null) {
            throw new IllegalStateException(
                "You called PostingsFormat.availablePostingsFormats() before all formats could be initialized. "
                    +
                    "This likely happens if you call it from a PostingsFormat's ctor.");
        }
        return loader.availableServices();
    }

    /**
     * Reloads the postings format list from the given {@link ClassLoader}. Changes to the postings
     * formats are visible after the method ends, all iterators
     * ({@link #availablePostingsFormats()},...) stay consistent.
     *
     * <p><b>NOTE:</b> Only new postings formats are added, existing ones are
     * never removed or replaced.
     *
     * <p><em>This method is expensive and should only be called for discovery
     * of new postings formats on the given classpath/classloader!</em>
     */
    public static void reloadPostingsFormats(ClassLoader classloader) {
        loader.reload(classloader);
    }
}
