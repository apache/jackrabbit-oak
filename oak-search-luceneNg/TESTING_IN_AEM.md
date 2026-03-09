<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Testing LuceneNg in AEM

This guide explains how to test the LuceneNg implementation in a local AEM instance.

## Prerequisites

- Local AEM instance running (author on port 4502)
- Admin credentials
- Oak version 1.93-SNAPSHOT or compatible

## Step 1: Build the Bundle

```bash
cd oak-search-luceneNg
mvn clean install -DskipTests -Drat.skip=true
```

The bundle will be created at: `target/oak-search-luceneNg-1.93-SNAPSHOT.jar`

## Step 2: Install the LuceneNg Bundle (Self-Contained!)

**Good news:** The LuceneNg bundle embeds Lucene 9.12.2, so no separate Lucene JARs are needed!

### Option A: Via Felix Console

1. Open http://localhost:4502/system/console/bundles
2. Click "Install/Update"
3. Upload `target/oak-search-luceneNg-1.93-SNAPSHOT.jar`
4. Click "Install or Update"
5. Verify the bundle is "Active"

### Option B: Via install folder

```bash
cp target/oak-search-luceneNg-1.93-SNAPSHOT.jar \
   <AEM_HOME>/crx-quickstart/install/
```

## Step 3: Verify Bundle Installation

1. Open http://localhost:4502/system/console/bundles
2. Search for "lucene"
3. Verify you see:
   - `Oak Lucene 9 (1.93-SNAPSHOT)` - Active

Note: You should NOT see separate Lucene bundles - Lucene 9.12.2 is embedded inside the LuceneNg bundle (6.4MB), following the same pattern as oak-lucene (embeds 4.7.2) and oak-search-elastic (embeds 9.12.2).

**How It Works:**
When the bundle activates, the `LuceneNgIndexProviderService` OSGi component:
- Registers `QueryIndexProvider` with property `type=lucene9`
- Registers `IndexEditorProvider` with property `type=lucene9`
- Oak uses these registrations to route index operations to LuceneNg when it encounters an index definition with `type=lucene9`

## Step 4: Create a Test Index Definition

### Via CRXDE Lite (http://localhost:4502/crx/de)

1. Navigate to `/oak:index`
2. Create a new node:
   - Name: `testLuceneNg`
   - Type: `oak:QueryIndexDefinition`

3. Add properties to `testLuceneNg`:
   ```
   type (String) = "lucene9"
   async (String) = "async"
   includedPaths (String[]) = ["/content"]
   ```

4. Save

### Via Groovy Console (http://localhost:4502/etc/groovy-console.html)

```groovy
def session = resourceResolver.adaptTo(javax.jcr.Session)
def indexNode = session.getNode('/oak:index')

// Create index definition
def testIndex = indexNode.addNode('testLuceneNg', 'oak:QueryIndexDefinition')
testIndex.setProperty('type', 'lucene9')
testIndex.setProperty('async', 'async')
testIndex.setProperty('includedPaths', ['/content'] as String[])

session.save()
println "Index created: /oak:index/testLuceneNg"
```

## Step 5: Create Test Content

```groovy
def session = resourceResolver.adaptTo(javax.jcr.Session)

// Create test pages
def content = session.getNode('/content')
def testPage = content.addNode('luceneNgTest', 'cq:Page')
def jcrContent = testPage.addNode('jcr:content', 'cq:PageContent')
jcrContent.setProperty('jcr:title', 'Oak LuceneNg Test')
jcrContent.setProperty('text', 'Testing Oak with Lucene 9 implementation')

def testPage2 = content.addNode('luceneNgTest2', 'cq:Page')
def jcrContent2 = testPage2.addNode('jcr:content', 'cq:PageContent')
jcrContent2.setProperty('jcr:title', 'Another Test')
jcrContent2.setProperty('text', 'More test content for Oak indexing')

session.save()
println "Test content created"
```

## Step 6: Trigger Async Indexing

The async indexer runs periodically. To force immediate indexing:

1. Go to http://localhost:4502/system/console/jmx
2. Find: `org.apache.jackrabbit.oak:name=async,type=IndexStats`
3. Click on it
4. Execute operation: `abortAndPause()`
5. Then execute: `resume()`

Or wait ~5 seconds for the async cycle to run automatically.

## Step 7: Verify Indexing

### Check Index Data

1. Open CRXDE Lite
2. Navigate to `/var/indexing/lucene/testLuceneNg`
3. You should see Lucene index files stored as chunks

### Check Logs

```bash
tail -f <AEM_HOME>/crx-quickstart/logs/error.log | grep -i luceneNg
```

Look for:
- `LuceneNgIndexEditor` messages about indexing
- `LuceneNgQueryIndexProvider` messages about queries

## Step 8: Test Queries

### Via Query Builder Debugger (http://localhost:4502/libs/cq/search/content/querydebug.html)

Query:
```
type=cq:Page
fulltext=Oak
```

### Via Groovy Console

```groovy
import javax.jcr.query.*

def session = resourceResolver.adaptTo(javax.jcr.Session)
def qm = session.getWorkspace().getQueryManager()

// Test full-text search
def query = qm.createQuery(
    "SELECT * FROM [cq:Page] WHERE CONTAINS(*, 'Oak')",
    Query.JCR_SQL2
)

def result = query.execute()
def nodes = result.getNodes()

println "Found ${nodes.size} results:"
while (nodes.hasNext()) {
    def node = nodes.nextNode()
    println "  - ${node.path}"
}
```

## Step 9: Verify LuceneNg is Used

### Check Query Explanation

1. Go to http://localhost:4502/system/console/jmx
2. Find: `org.apache.jackrabbit.oak:name=QueryEngineSettings,type=QueryEngineSettings`
3. Set `FullTextComparisonWithoutIndex` to `false`
4. In Query Builder Debugger, check "Explain" checkbox
5. Look for "lucene9" or "testLuceneNg" in the query plan

### Check Logs

Enable debug logging:

1. Go to http://localhost:4502/system/console/slinglog
2. Create new logger:
   - Log Level: DEBUG
   - Logger: `org.apache.jackrabbit.oak.plugins.index.luceneNg`
3. Run queries and check logs

## Troubleshooting

### Bundle Not Starting

Check Felix console for missing dependencies:
```
http://localhost:4502/system/console/bundles
```

### No Results from Queries

1. Verify index definition: `/oak:index/testLuceneNg`
2. Check async indexing status:
   ```
   http://localhost:4502/system/console/jmx
   -> org.apache.jackrabbit.oak:name=async,type=IndexStats
   ```
3. Check index data exists: `/var/indexing/lucene/testLuceneNg/`
4. Enable debug logs

### Index Not Being Used

1. Check query plan (explain query)
2. Verify `includedPaths` covers your content
3. Check index cost calculation in logs

## Expected Results

- ✅ Bundle installs and starts successfully
- ✅ Index definition with `type=lucene9` is recognized
- ✅ Documents are indexed to `/var/indexing/lucene/`
- ✅ Full-text queries return correct results
- ✅ Query explain shows `lucene9` index is used

## Next Steps

After basic testing works:
1. Create more complex index definitions
2. Test different query types
3. Monitor performance
4. Compare with legacy Lucene 4.7 indexes
