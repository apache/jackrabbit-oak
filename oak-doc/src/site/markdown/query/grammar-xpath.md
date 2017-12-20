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

## Oak XPath Query Grammar

### XPath

* 

### SQL-2

* [Query](#query)
* [Explain](#explain)
* [Measure](#measure)


<h3 id="query">Query</h3>

<h4>
SELECT [ DISTINCT ] { * | { <a href="#column">column</a> [ , ... ] } }
<br/> FROM { <a href="#selector">selector</a> [ <a href="#join">join</a> ... ] }
<br/> [ WHERE <a href="#constraint">constraint</a> ]
<br/> [ UNION [ ALL ] <a href="#query">query</a> ]
<br/> [ ORDER BY { <a href="#ordering">ordering</a> [ , ... ] } ]
<br/> [ <a href="#options">queryOptions</a> ]
</h4>

DISTINCT ensures each row is only returned once.

UNION combines the result of this query with the results of another query,
where UNION ALL does not remove duplicate.

ORDER BY may use an index.
If there is no index for the given sort order, 
then the result is fully read in memory and sorted before returning the first row.

<h3 id="options">Options</h3>

<h4>
OPTION( { 
<br/>&nbsp;&nbsp; TRAVERSAL { OK | WARN | FAIL | DEFAULT } | 
<br/>&nbsp;&nbsp; INDEX TAG tagName 
<br/> } [ , ... ] )
</h4>

TRAVERSAL: by default, queries without index will log a warning,
except if the configuration option `QueryEngineSettings.failTraversal` is changed
The traversal option can be used to change the behavior of the given query:
OK to not log a warning,
WARN to log a warning,
FAIL to fail the query, and 
DEFAULT to use the default setting.

INDEX TAG: by default, queries will use the index with the lowest expected cost (as in relational databases).
To only consider some of the indexes, add tags (a multi-valued String property) to the index(es) of choice,
and specify this tag in the query.

<h3 id="explain">Explain Query</h3>

<h4>
EXPLAIN [MEASURE] { <a href="#query">query</a> }
</h4>

Does not run the query, but only computes and returns the query plan.
With EXPLAIN MEASURE, the expected cost is calculated as well.
In both cases, the query result will only have one column called 'plan', and one row that contains the plan.

Examples:

    EXPLAIN MEASURE 
    SELECT * FROM [nt:base] WHERE [jcr:uuid] = 1

Result:

    plan = [nt:base] as [nt:base] 
    /* property uuid = 1 where [nt:base].[jcr:uuid] = 1 */  
    cost: { "nt:base": 2.0 } 

This means the property index named "uuid" is used for this query.
The expected cost (roughly the number of uncached I/O operations) is 2.


<h3 id="measure">Measure</h3>

<h4>
MEASURE { <a href="#query">query</a> }
</h4>

Runs the query, but instead of returning the result, returns the number of rows traversed.
The query result has two columns, one called 'selector' and one called 'scanCount'.
The result has at least two rows, one that represents the total (selector set to 'query'),
and one per selector used in the query. 

Examples:

    MEASURE 
    SELECT * FROM [nt:base] WHERE [jcr:uuid] = 1

Result:

    selector = query
    scanCount = 0
    selector = nt:base
    scanCount = 0

In this case, the scanCount is zero because the query did not find any nodes.

