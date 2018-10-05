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

## Flags

List of available flags to enable/disable options in the query engine

#### oak.queryFullTextComparisonWithoutIndex
`@since 1.2.0` 

Default is `false`. If provided on the command line like
`-Doak.queryFullTextComparisonWithoutIndex=true` it will allow the
query engine to parse full text conditions even if no full-text
indexes are defined.

#### oak.query.sql2optimisation
`@since 1.3.9, 1.3.11`

It will perform another round of optimisation to the provided
query. See the [related section in Query Engine](query-engine.html#SQL2_Optimisation) page.

