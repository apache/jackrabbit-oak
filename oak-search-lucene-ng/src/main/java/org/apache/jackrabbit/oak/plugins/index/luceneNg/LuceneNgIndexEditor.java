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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.editor.LuceneNgIndexEditorContext;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditor;
import org.apache.lucene.document.Document;

/**
 * IndexEditor for Lucene 9. Thin subclass of the shared {@link FulltextIndexEditor} — see that
 * class (and {@code oak-lucene}'s {@code LuceneIndexEditor}, the same pattern for the legacy
 * module) for the tree-traversal, index-time aggregation, and rule-transition tracking behaviour
 * this class inherits rather than reimplementing.
 *
 * <p>The root editor's {@code leave()} closes the writer via {@code context.closeWriter()} in the
 * base class (see {@link FulltextIndexEditor#leave}), so no override is needed here.</p>
 */
public class LuceneNgIndexEditor extends FulltextIndexEditor<Document> {

    LuceneNgIndexEditor(LuceneNgIndexEditorContext context) {
        super(context);
    }
}
