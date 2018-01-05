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
* [Options](#options)
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
where UNION ALL does not remove duplicates.

ORDER BY may use an index.
If there is no index for the given sort order, 
then the result is fully read in memory and sorted before returning the first row.

<h3 id="column">Column</h3>

<h4>
{ [ selectorName . ] propertyName
<br/> | selectorName . *
<br/> | EXCERPT([selectorName])
<br/> | rep:spellcheck()
<br/> } [ AS aliasName ]
</h4>


<h3 id="selector">Selector</h3>

<h4>
nodeTypeName [ AS selectorName ]
</h4>

The nodetype name can be either a primary nodetype or a mixin nodetype.



<h3 id="join">Join</h3>

<h4>
{ INNER | { LEFT | RIGHT } OUTER } JOIN rightSelector ON
<br/> { selectorName . propertyName = joinSelectorName . joinPropertyName }
<br/> | { ISSAMENODE( selectorName , joinSelectorName [ , selectorPathName ] ) }
<br/> | { ISCHILDNODE( childSelectorName , parentSelectorName ) }
<br/> | { ISDESCENDANTNODE( descendantSelectorName , ancestorSelectorName ) }
</h4>

An inner join only returns entries if nodes are found on both the left and right selector.
A left outer join will return entries that don't have matching nodes on the right selector.
A right outer join will return entries that don't have matching nodes on the left selector.
For outer joins, all the properties of the selector that doesn't have a matching node are null.


<h3 id="constraint">Constraint</h3>

<h4>
andCondition [ { OR andCondition } [...] ]
</h4>

OR conditions of the form "X = 1 OR X = 2" are automatically converted to "X IN(1, 2)".

OR conditions of the form "X = 1 OR Y = 2" are more complicated.
Oak will try two options: first, what is the expected cost to use a UNION query
(one query with X = 1, and a second query with Y = 2).
If using UNION results in a lower estimated cost, then UNION is used.
This can be the case, for example, if there are two distinct indexes,
one on X and another on Y.


<h3 id="andCondition">And Condition</h3>

<h4>
condition [ { AND condition } [...] ]
</h4>

A special case (not found in relational databases) is
AND conditions of the form "X = 1 AND X = 2".
They will match nodes with multi-valued properties, 
where the property value contains both 1 and 2.

<h3 id="condition">Condition</h3>

<h4>
comparison
<br/> inComparison
<br/> | NOT constraint
<br/> | ( constraint )
<br/> | [ selectorName . ] propertyName IS [ NOT ] NULL
<br/> | CONTAINS( { { [ selectorName . ] propertyName } | { selectorName . * } } , fulltextSearchExpression )
<br/> | { ISSAMENODE | ISCHILDNODE | ISDESCENDANTNODE } (  [ selectorName , ] pathString )
<br/> | SIMILAR ( [ selectorName . ] { propertyName | * } , staticOperand )
<br/> | NATIVE ( [ selectorName , ] language , staticOperand )
<br/> | SPELLCHECK ( [ selectorName , ] staticOperand )
<br/> | SUGGEST ( [ selectorName , ] staticOperand )
</h4>


<h3 id="comparison">Comparison</h3>

<h4>
dynamicOperand { = | &lt;&gt; | &lt; | &lt;= | &gt; | &gt;= | LIKE } staticOperand
</h4>


<h3 id="inComparison">In Comparison</h3>

<h4>
dynamicOperand IN ( staticOperand [, ...] )
</h4>


<h3 id="staticOperand">Static Operand</h3>

<h4>
literal
<br/> | $ bindVariableName
<br/> | CAST ( literal AS { 
<br/>&nbsp;&nbsp; STRING 
<br/>&nbsp;&nbsp; | BINARY 
<br/>&nbsp;&nbsp; | DATE 
<br/>&nbsp;&nbsp; | LONG 
<br/>&nbsp;&nbsp; | DOUBLE 
<br/>&nbsp;&nbsp; | DECIMAL 
<br/>&nbsp;&nbsp; | BOOLEAN 
<br/>&nbsp;&nbsp; | NAME 
<br/>&nbsp;&nbsp; | PATH 
<br/>&nbsp;&nbsp; | REFERENCE 
<br/>&nbsp;&nbsp; | WEAKREFERENCE 
<br/>&nbsp;&nbsp; | URI } )
</h4>


<h3 id="ordering">Ordering</h3>

<h4>
dynamicOperand [ ASC | DESC ]
</h4>

Ordering by an indexed property will use that index if possible.
If there is no index that can be used for the given sort order,
then the result is fully read in memory and sorted there.

As a special case, sorting by "jcr:score" in descending order is ignored 
(removed from the list), as this is what the fulltext index does anyway
(and if no fulltext index is used, then the score doesn't apply).
If for some reason you want to enforce sorting by "jcr:score", then
you can use the workaround to order by "LOWER([jcr:score]) DESC".

<h3 id="dynamicOperand">Dynamic Operand</h3>

<h4>
[ selectorName . ] propertyName
<br/>  | LENGTH( dynamicOperand  )
<br/>  | { NAME | LOCALNAME | SCORE } ( [ selectorName ] )
<br/>  | { LOWER | UPPER } ( dynamicOperand )
<br/>  | COALESCE ( dynamicOperand1, dynamicOperand2 )
<br/>  | PROPERTY ( propertyName, type )
</h4>

The selector name is only needed if the query contains multiple selectors.

COALESCE: this returns the first operand if it is not null,
and the second operand otherwise.
`@since Oak 1.8`

PROPERTY: This feature is rarely used. 
It allows to filter for all properties with a given type.
Example: the condition `PROPERTY(*, Reference) = $uuid` will search for any property of type
`Reference`.

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

