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

## Oak SQL-2 Query Grammar

* [Query](#query)
* [Column](#column)
* [Selector](#selector)
* [Join](#join)
* [Constraint](#constraint)
* [And Condition](#andCondition)
* [Condition](#condition)
* [Comparison](#comparison)
* [In Comparison](#inComparison)
* [Static Operand](#staticOperand)
* [Ordering](#ordering)
* [Dynamic Operand](#dynamicOperand)
* [Type](#type)
* [Options](#options)
* [Explain](#explain)
* [Measure](#measure)

<hr />
<h3 id="query">Query</h3>

<h4>
SELECT [ DISTINCT ] { * | { <a href="#column">column</a> [ , ... ] } }
<br/> FROM { <a href="#selector">selector</a> [ <a href="#join">join</a> ... ] }
<br/> [ WHERE <a href="#constraint">constraint</a> ]
<br/> [ UNION [ ALL ] <a href="#query">query</a> ]
<br/> [ ORDER BY { <a href="#ordering">ordering</a> [ , ... ] } ]
<br/> [ <a href="#options">queryOptions</a> ]
</h4>

All queries should have a path restriction 
(even if it's just, for example, "/content"), as this allows to shrink indexes.

"distinct" ensures each row is only returned once.

"union" combines the result of this query with the results of another query,
where "union all" does not remove duplicates.

"order by" may use an index.
If there is no index for the given sort order, 
then the result is fully read in memory and sorted before returning the first row.

Examples:

    select * from [sling:Folder] as a where [sling:resourceType] = 'x' and isdescendantnode(a, '/content')
    select [jcr:path] from [oak:QueryIndexDefinition] as a where [type] = 'lucene' and isdescendantnode(a, '/') order by [reindexCount] desc
    select [jcr:path], [jcr:score], * from [nt:base] as a where [type] = 'report' and isdescendantnode(a, '/etc') option(traversal fail)

<hr />
<h3 id="column">Column</h3>

<h4>
{ [ selectorName . ] { propertyName | * }
<br/> | EXCERPT([selectorName])
<br/> | rep:spellcheck()
<br/> } [ AS aliasName ]
</h4>

It is recommended to enclose property names in square brackets.

Not listed above are "special" properties such as "[jcr:path]" (the path), "[jcr:score]" (the score),
"[rep:suggest()]".

Examples:

    *
    [jcr:path]
    [jcr:score]
    a.*
    a.[sling:resourceType]

<hr />
<h3 id="selector">Selector</h3>

<h4>
nodeTypeName [ AS selectorName ]
</h4>

The nodetype name can be either a primary nodetype or a mixin nodetype.
It is recommended to specify the nodetype name in square brackes.

Examples:

    [sling:Folder] as a

<hr />
<h3 id="join">Join</h3>

<h4>
{ INNER | { LEFT | RIGHT } OUTER } JOIN <a href="#selector">rightSelector</a> ON
<br/> { selectorName . propertyName = joinSelectorName . joinPropertyName }
<br/> | { ISSAMENODE( selectorName , joinSelectorName [ , selectorPathName ] ) }
<br/> | { ISCHILDNODE( childSelectorName , parentSelectorName ) }
<br/> | { ISDESCENDANTNODE( descendantSelectorName , ancestorSelectorName ) }
</h4>

An "inner join" only returns entries if nodes are found on both the left and right selector.
A "left outer join" will return entries that don't have matching nodes on the right selector.
A "right outer join" will return entries that don't have matching nodes on the left selector.
For outer joins, all the properties of the selector that doesn't have a matching node are null.

Examples:

All nodes below /oak:index that _don't_ have a child node:

    select a.* from [oak:QueryIndexDefinition] as a 
      left outer join [nt:base] as b on ischildnode(b, a)
      where isdescendantnode(a, '/oak:index') 
      and b.[jcr:primaryType] is null 
      order by a.[jcr:path]


<hr />
<h3 id="constraint">Constraint</h3>

<h4>
<a href="#andCondition">andCondition</a> [ { OR <a href="#andCondition">andCondition</a> } [...] ]
</h4>

"or" conditions of the form "[x]=1 or [x]=2" are automatically converted to "[x] in(1, 2)",
and can use the same an index.

"or" conditions of the form "[x]=1 or [y]=2" are more complicated.
Oak will try two options: first, what is the expected cost to use a "union" query
(one query with x=1, and a second query with y=2).
If using "union" results in a lower estimated cost, then "union" is used.
This can be the case, for example, if there are two distinct indexes,
one on x, and another on y.

<hr />
<h3 id="andCondition">And Condition</h3>

<h4>
<a href="#condition">condition</a> [ { AND <a href="#condition">condition</a> } [...] ]
</h4>

A special case (not found in relational databases) is
"and" conditions of the form "[x]=1 and [x]=2".
They will match nodes with multi-valued properties, 
where the property value contains both 1 and 2.

<hr />
<h3 id="condition">Condition</h3>

<h4>
<a href="#comparison">comparison</a>
<br/> <a href="#inComparison">inComparison</a>
<br/> | NOT <a href="#constraint">constraint</a>
<br/> | ( <a href="#constraint">constraint</a> )
<br/> | [ selectorName . ] propertyName IS [ NOT ] NULL
<br/> | CONTAINS( { { [ selectorName . ] propertyName } | { selectorName . * } } , staticOperand )
<br/> | { ISSAMENODE | ISCHILDNODE | ISDESCENDANTNODE } (  [ selectorName , ] pathString )
<br/> | SIMILAR ( [ selectorName . ] { propertyName | * } , staticOperand )
<br/> | NATIVE ( [ selectorName , ] language , staticOperand )
<br/> | SPELLCHECK ( [ selectorName , ] staticOperand )
<br/> | SUGGEST ( [ selectorName , ] staticOperand )
</h4>

"not" conditions can not typically use an index.

"contains": see <a href="query-engine.html#Full-Text_Queries">Full-Text Queries</a>.

"similar": see <a href="query-engine.html#Similarity_Queries">Similarity Queries</a>.

"native": see <a href="query-engine.html#Native_Queries">Native Queries</a>.

"spellcheck": see <a href="query-engine.html#Spellchecking">Spellchecking</a>.

"suggest": see <a href="query-engine.html#Suggestions">Suggestions</a>.

Examples:

    select [jcr:path] from [nt:base] where similar(*, '/test/a') 
    select [jcr:path] from [nt:base] where native('solr', 'name:(Hello OR World)')
    select [rep:suggest()] from [nt:base] where suggest('in ') and issamenode('/')
    select [rep:spellcheck()] from [nt:base] as a where spellcheck('helo') and issamenode(a, '/')


<hr />
<h3 id="comparison">Comparison</h3>

<h4>
<a href="#dynamicOperand">dynamicOperand</a> 
{ = | &lt;&gt; | &lt; | &lt;= | &gt; | &gt;= | LIKE } 
<a href="#staticOperand">staticOperand</a>
</h4>

"like": when comparing with LIKE, the wildcards characters are '_' (any one character) 
and '%' (any characters). An index is used, 
except if the operand starts with a wildcard. 
To search for the characters '%' and '_', the characters need to be escaped using '\' (backslash).

Comparison using &lt;, &gt;, &gt;=, and &lt;= can use an index if the property in the index is ordered.

Examples:

    [name] like '%: 100 \%'


<hr />
<h3 id="inComparison">In Comparison</h3>

<h4>
<a href="#dynamicOperand">dynamicOperand</a> IN ( <a href="#staticOperand">staticOperand</a> [, ...] )
</h4>

Examples:

    [status] in('active', 'inactive')


<hr />
<h3 id="staticOperand">Static Operand</h3>

<h4>
literal
<br/> | $ bindVariableName
<br/> | CAST ( literal AS <a href="#type">type</a> )
</h4>

A string (text) literal starts and ends with a single quote. 
Two single quotes can be used to create a single quote inside a string.

Example:

    'John''s car'
    $uuid
    cast('2020-12-01T20:00:00.000' as date)


<hr />
<h3 id="ordering">Ordering</h3>

<h4>
<a href="#dynamicOperand">dynamicOperand</a> [ ASC | DESC ]
</h4>

Ordering by an indexed property will use that index if possible.
If there is no index that can be used for the given sort order,
then the result is fully read in memory and sorted there.

As a special case, sorting by "jcr:score" in descending order is ignored 
(removed from the list), as this is what the fulltext index does anyway
(and if no fulltext index is used, then the score doesn't apply).
If for some reason you want to enforce sorting by "jcr:score", then
you can use the workaround to order by "LOWER([jcr:score]) DESC".

Examples:

    [lastName]
    [price] desc


<hr />
<h3 id="dynamicOperand">Dynamic Operand</h3>

<h4>
[ selectorName . ] propertyName
<br/>  | LENGTH( dynamicOperand  )
<br/>  | { NAME | LOCALNAME | SCORE } ( [ selectorName ] )
<br/>  | { LOWER | UPPER } ( dynamicOperand )
<br/>  | COALESCE ( dynamicOperand1, dynamicOperand2 )
<br/>  | PROPERTY ( propertyName, <a href="#type">type</a> )
</h4>

The selector name is only needed if the query contains multiple selectors.

"coalesce": this returns the first operand if it is not null,
and the second operand otherwise.
`@since Oak 1.8`

"property": This feature is rarely used. 
It allows to filter for all properties with a given type.
Example: the condition `property(*, Reference) = $uuid` will search for any property of type
`Reference`.

"lower", "upper", "length": Indexes on functions are supported `@since Oak 1.6`, see OAK-3574.

Examples:

    lower([firstName])
    coalesce([lastName], name())
    length(coalesce([lastName], name()))


<hr />
<h3 id="type">Type</h3>

<h4>
<br/>STRING 
<br/> | BINARY 
<br/> | DATE 
<br/> | LONG 
<br/> | DOUBLE 
<br/> | DECIMAL 
<br/> | BOOLEAN 
<br/> | NAME 
<br/> | PATH 
<br/> | REFERENCE 
<br/> | WEAKREFERENCE 
<br/> | URI
</h4>

This is the list of all JCR property types.


<hr />
<h3 id="options">Options</h3>

<h4>
OPTION( { 
<br/>&nbsp;&nbsp; TRAVERSAL { OK | WARN | FAIL | DEFAULT } | 
<br/>&nbsp;&nbsp; INDEX TAG tagName 
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
EXPLAIN [MEASURE] { <a href="#query">query</a> }
</h4>

Does not run the query, but only computes and returns the query plan.
With EXPLAIN MEASURE, the expected cost is calculated as well.
In both cases, the query result will only have one column called 'plan', and one row that contains the plan.

Examples:

    explain measure 
    select * from [nt:base] where [jcr:uuid] = 1

Result:

    plan = [nt:base] as [nt:base] 
    /* property uuid = 1 where [nt:base].[jcr:uuid] = 1 */  
    cost: { "nt:base": 2.0 } 

This means the property index named "uuid" is used for this query.
The expected cost (roughly the number of uncached I/O operations) is 2.


<hr />
<h3 id="measure">Measure</h3>

<h4>
MEASURE { <a href="#query">query</a> }
</h4>

Runs the query, but instead of returning the result, returns the number of rows traversed.
The query result has two columns, one called 'selector' and one called 'scanCount'.
The result has at least two rows, one that represents the total (selector set to 'query'),
and one per selector used in the query. 

Examples:

    measure 
    select * from [nt:base] where [jcr:uuid] = 1

Result:

    selector = query
    scanCount = 0
    selector = nt:base
    scanCount = 0

In this case, the scanCount is zero because the query did not find any nodes.

