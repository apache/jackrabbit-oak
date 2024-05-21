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

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;

/**
 * The core terms dictionaries (BlockTermsReader, BlockTreeTermsReader) interact with a single
 * instance of this class to manage creation of {@link DocsEnum} and {@link DocsAndPositionsEnum}
 * instances.  It provides an IndexInput (termsIn) where this class may read any previously stored
 * data that it had written in its corresponding {@link PostingsWriterBase} at indexing time.
 *
 * @lucene.experimental
 */

// TODO: find a better name; this defines the API that the
// terms dict impls use to talk to a postings impl.
// TermsDict + PostingsReader/WriterBase == PostingsConsumer/Producer
public abstract class PostingsReaderBase implements Closeable {

    /**
     * Sole constructor. (For invocation by subclass constructors, typically implicit.)
     */
    protected PostingsReaderBase() {
    }

    /**
     * Performs any initialization, such as reading and verifying the header from the provided terms
     * dictionary {@link IndexInput}.
     */
    public abstract void init(IndexInput termsIn) throws IOException;

    /**
     * Return a newly created empty TermState
     */
    public abstract BlockTermState newTermState() throws IOException;

    /**
     * Actually decode metadata for next term
     *
     * @see PostingsWriterBase#encodeTerm
     */
    public abstract void decodeTerm(long[] longs, DataInput in, FieldInfo fieldInfo,
        BlockTermState state, boolean absolute) throws IOException;

    /**
     * Must fully consume state, since after this call that TermState may be reused.
     */
    public abstract DocsEnum docs(FieldInfo fieldInfo, BlockTermState state, Bits skipDocs,
        DocsEnum reuse, int flags) throws IOException;

    /**
     * Must fully consume state, since after this call that TermState may be reused.
     */
    public abstract DocsAndPositionsEnum docsAndPositions(FieldInfo fieldInfo, BlockTermState state,
        Bits skipDocs, DocsAndPositionsEnum reuse,
        int flags) throws IOException;

    /**
     * Returns approximate RAM bytes used
     */
    public abstract long ramBytesUsed();

    @Override
    public abstract void close() throws IOException;
}
