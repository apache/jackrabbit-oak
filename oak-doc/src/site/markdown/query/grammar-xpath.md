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

* [Query](#query)
* [Filter](#filter)
* [Column](#column)
* [Constraint](#constraint)
* [And Condition](#andCondition)
* [Condition](#condition)
* [Comparison](#comparison)
* [Static Operand](#staticOperand)
* [Ordering](#ordering)
* [Dynamic Operand](#dynamicOperand)
* [Options](#options)
* [Explain](#explain)
* [Measure](#measure)

<hr />
<h3 id="query">Query</h3>

<h4>
/jcr:root { <a href="#filter">filter</a> [ ... ] } }
<br/> [ ( <a href="#column">column</a> [ | ... ] } ) ]
<br/> [ order by { <a href="#ordering">ordering</a> [ , ... ] } ]
<br/> [ <a href="#options">queryOptions</a> ]
</h4>

The "/jcr:root" means the root node. 
It is recommended that all XPath queries start with this term.

All queries should have a path restriction 
(even if it's just, for example, "/content"), as this allows to shrink indexes.

"order by" may use an index.
If there is no index for the given sort order, 
then the result is fully read in memory and sorted before returning the first row.

The column list is usually not needed, as all properties of the nodes are returned in any case.
It is only needed if non-standard (computed) columns such as 
"rep:excerpt" or "rep:spellcheck" are needed.

Examples:

Get all nodes of node type 'sling:Folder' with the property 'sling:resourceType' set to 'x':

    /jcr:root/content//element(*, sling:Folder)[@sling:resourceType='x']

Get all index definition nodes of type 'lucene', sorted by reindex count, descending:

    /jcr:root/oak:index/element(*, oak:QueryIndexDefinition)[@type='lucene'] order by @reindexCount descending

Get all nodes below /etc that have the property type set to 'report'; fail the query if there is no index that can be used:

    /jcr:root/etc//*[@type='report'] option(traversal fail)


<hr />
<h3 id="filter">Filter</h3>

<h4>
/ <a href="#filter">directChildNodeFilter</a>
<br/> | // <a href="#filter">descendantNodeFilter</a>
<br/> | *
<br/> | element( [ * | nodeName ] [, nodeType] )
<br/> | text()
<br/> | '[' <a href="#constraint">constraint</a> ']'
<br/> | ( filter '|' unionFilter [...] )
</h4>

A single slash means filtering on a specific child node,
while two slashes means filtering on a descendant node.

"*" means any node name, and any node type.

"text()" is a shortcut for "jcr:xmltext". 
It is supported only for compatibility.

Queries using the construct `(filter1 | filter2)' are converted to union,
that is, one query is generated for each filter, and the result of both
queries is combined.

Examples:

Only direct child nodes of /content/dam:

    /jcr:root/content/dam/*

All descendant nodes of /content/oak:index:

    /jcr:root/content/oak:index//*

All nodes named "rep:policy" that have a parent node which is the direct child node of /home/users:

    /jcr:root/home/users/*/rep:policy

All nodes named "profile" that have a parent node which is a descendant of /home/users:

    /jcr:root/home/users//*/profile

All descendant nodes of /content that are of type oak:QueryIndexDefinition:

    /jcr:root/content//element(*, oak:QueryIndexDefinition)

All descendant nodes of /content that are of type oak:QueryIndexDefinition and are named "lucene":

    /jcr:root/content//element(lucene, oak:QueryIndexDefinition)

All nodes named "indexRules" that have a parent node with the property "type" set to "lucene":

    /jcr:root/oak:index/*[@type = 'lucene']/indexRules
    
The paths '/libs' and '/etc' should be searched:

    /jcr:root/(libs | etc)//*[@jcr:uuid and @jcr:mimeType = 'text/css']


<hr />
<h3 id="column">Column</h3>

<h4>
rep:excerpt( [. | [ relativePath / ] @propertyName ]  )
<br/> | rep:spellcheck()
<br/> | rep:suggest( [.] )
<br/> | rep:facet ( [ relativePath / ] @propertyName )
</h4>

"rep:excerpt": include the spellcheck column in the result.
Since Oak version 1.8.1, optionally a property name can be specified.
See also <a href="lucene.html#Property_Definitions">useInExcerpt</a>.

"rep:spellcheck": Include the spellcheck in the result.
See <a href="query-engine.html#Spellchecking">Spellchecking</a>.

"rep:suggest": include suggestions in the result.
See <a href="query-engine.html#Suggestions">Suggestions</a>.

"rep:facet": include facets in the result.
See <a href="query-engine.html#Facets">Facets</a>.

Examples:

    /jcr:root/content//*[rep:suggest('in ')]/(rep:suggest())

    /jcr:root/content//*[jcr:contains(@jcr:title, 'oak')]/(rep:facet(@tags))


<hr />
<h3 id="constraint">Constraint</h3>

<h4>
<a href="#andCondition">andCondition</a> [ { or <a href="#andCondition">andCondition</a> } [...] ]
</h4>

"or" conditions of the form "@x = 1 or @x = 2" can use the same index.

"or" conditions of the form "@x = 1 or @y = 2" are more complicated.
Oak will convert them to a "union" query
(one query with @x = 1, and a second query with @y = 2).

Examples:

Include the nodes that have the property 'hidden' set to 'hidden-folder',
or don't have the property set:

    /jcr:root/content/dam/*[@hidden='hidden-folder' or not(@hidden)]


<hr />
<h3 id="andCondition">And Condition</h3>

<h4>
<a href="#condition">condition</a> [ { and <a href="#condition">condition</a> } [...] ]
</h4>

A special case (not found in relational databases) is
"and" conditions of the form "@x = 1 and @x = 2".
They will match nodes with multi-valued properties, 
where the property value contains both 1 and 2.

Examples:

    /jcr:root/home//element(*, rep:Authorizable)[@rep:principalName != 'Joe' and @rep:principalName != 'Steve']


<hr />
<h3 id="condition">Condition</h3>

<h4>
<a href="#comparison">comparison</a>
<br/> <a href="#inComparison">inComparison</a>
<br/> | [ [ fn:not | not ] (<a href="#constraint">constraint</a>)
<br/> | ( <a href="#constraint">constraint</a> )
<br/> | jcr:contains( [ { property | . } , ] fulltextSearchExpression )
<br/> | jcr:like( dynamicOperand , staticOperand )
<br/> | rep:similar ( propertyName , staticOperand )
<br/> | rep:native ( language , staticOperand )
<br/> | rep:spellcheck ( staticOperand )
<br/> | rep:suggest ( staticOperand )
</h4>

"fn:not": this is used for both "is not null", and for nested conditions.
For example "fn:not(@status)" means nodes where this property is not set.
In this case, and index can be used, but it is relatively expensive to index
all nodes that don't have a certain property. It is recommended to only index
this if there are relatively view nodes with this nodetype. See also
<a href="lucene.html">nullCheckEnabled</a>.
Nested conditions, for example "fn:not(@status = 'x' and @color = 'red')", 
can not typically use an index.

"jcr:contains": see <a href="query-engine.html#Full-Text_Queries">Full-Text Queries</a>.

"jcr:like": the wildcards characters are _ (any one character) 
and % (any characters). An index is used,
except if the operand starts with a wildcard. 
To search for the characters % and _, the characters need to be escaped using \ (backslash).

"rep:similar": see <a href="query-engine.html#Similarity_Queries">Similarity Queries</a>.

"rep:native": see <a href="query-engine.html#Native_Queries">Native Queries</a>.

"rep:spellcheck": see <a href="query-engine.html#Spellchecking">Spellchecking</a>.

"rep:suggest": see <a href="query-engine.html#Suggestions">Suggestions</a>.

Examples:

    /jcr:root/var/eventing//element(*, slingevent:Job)[@event.job.topic = 'abc' and not(@slingevent:finishedState)]

    /jcr:root/content//element(*, cq:Page)[@offTime > xs:dateTime('2020-12-01T20:00:00.000') or @onTime > xs:dateTime('2020-12-01T20:00:00.000')]


<hr />
<h3 id="comparison">Comparison</h3>

<h4>
<a href="#dynamicOperand">dynamicOperand</a> 
{ = | &lt;&gt; | != | &lt; | &lt;= | &gt; | &gt;= } 
<a href="#staticOperand">staticOperand</a>
</h4>

Comparison using &lt;, &gt;, &gt;=, and &lt;= can use an index if the property in the index is ordered.

Examples:

    @jcr:primaryType != 'nt:base'
    @offTime > xs:dateTime('2020-12-01T20:00:00.000')


<hr />
<h3 id="staticOperand">Static Operand</h3>

<h4>
textLiteral
<br/> | $ bindVariableName
<br/> | [ + | - ] numberLiteral
<br/> | true [ () ]
<br/> | false [ () ]
<br/> | xs:dateTime ( literal )
</h4>

A string (text) literal starts and ends with a single quote. 
Two single quotes can be used to create a single quote inside a string.

Examples:

    'John''s car'
    true()
    false
    1000
    -30.3
    xs:dateTime('2020-12-01T20:00:00.000')

<hr />
<h3 id="ordering">Ordering</h3>

<h4>
<a href="#dynamicOperand">dynamicOperand</a> [ ascending | descending ]
</h4>

Ordering by an indexed property will use that index if possible.
If there is no index that can be used for the given sort order,
then the result is fully read in memory and sorted there.

As a special case, sorting by "jcr:score" in descending order is ignored 
(removed from the list), as this is what the fulltext index does anyway
(and if no fulltext index is used, then the score doesn't apply).
If for some reason you want to enforce sorting by "jcr:score", then
you can use the workaround to order by "fn:lowercase(@jcr:score) descending".

Examples:

    order by @jcr:created descending


<hr />
<h3 id="dynamicOperand">Dynamic Operand</h3>

<h4>
[ relativePath / ] { @propertyName | * }
<br/>  | fn:string-length ( dynamicOperand  )
<br/>  | { fn:name | fn:local-name } ( [ . ] )
<br/>  | jcr:score ( )
<br/>  | { fn:lower-case | fn:upper-case } ( dynamicOperand )
<br/>  | fn:coalesce ( dynamicOperand1, dynamicOperand2 )
</h4>

The "*" stands for any property.

"jcr:score()" is the score returned by the index.

"fn:coalesce": this returns the first operand if it is not null,
and the second operand otherwise.
`@since Oak 1.8`

Examples:

    @type
    ./@jcr:primaryType
    items/type/@metaType
    indexRules/nt:base/*
    fn:string-length(@title)
    fn:name()
    jcr:score ()
    fn:lower-case(@lastName)
    fn:coalesce(@lastName, @name)


<hr />
<h3 id="options">Options</h3>

<h4>
option( { 
<br/>&nbsp;&nbsp; traversal { ok | warn | fail | default } | 
<br/>&nbsp;&nbsp; index tag tagName 
<br/> } [ , ... ] )
</h4>

"traversal": by default, queries without index will log a warning,
except if the configuration option `QueryEngineSettings.failTraversal` is changed
The traversal option can be used to change the behavior of the given query:
"ok" to not log a warning,
"warn" to log a warning,
"fail" to fail the query, and 
"default" to use the default setting.

"index tag": by default, queries will use the index with the lowest expected cost (as in relational databases).
To only consider some of the indexes, add tags (a multi-valued String property) to the index(es) of choice,
and specify this tag in the query.

Examples:

    option(traversal fail)


<hr />
<h3 id="explain">Explain Query</h3>

<h4>
explain [measure] { <a href="#query">query</a> }
</h4>

Does not run the query, but only computes and returns the query plan.
With "explain measure", the expected cost is calculated as well.
In both cases, the query result will only have one column called 'plan', and one row that contains the plan.

Examples:

    exlplain measure /jcr:root//*[@jcr:uuid = 'x']

Result:

    plan = [nt:base] as [nt:base] 
    /* property uuid = 1 where [nt:base].[jcr:uuid] = 1 */  
    cost: { "nt:base": 2.0 } 

This means the property index named "uuid" is used for this query.
The expected cost (roughly the number of uncached I/O operations) is 2.

<hr />
<h3 id="measure">Measure</h3>

<h4>
measure { <a href="#query">query</a> }
</h4>

Runs the query, but instead of returning the result, returns the number of rows traversed.
The query result has two columns, one called 'selector' and one called 'scanCount'.
The result has at least two rows, one that represents the total (selector set to 'query'),
and one per selector used in the query. 

Examples:

    measure /jcr:root//*[@jcr:uuid = 'x']

Result:

    selector = query
    scanCount = 0
    selector = nt:base
    scanCount = 0

In this case, the scanCount is zero because the query did not find any nodes.

