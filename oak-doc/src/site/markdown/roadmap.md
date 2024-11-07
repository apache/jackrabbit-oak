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

Roadmap
=======

Release Model
-------------
Following the release of Oak 1.10 we have started to make (even-numbered) stable
releases directly from [trunk](https://github.com/apache/jackrabbit-oak/tree/trunk/):

* [download](https://jackrabbit.apache.org/jcr/downloads.html#latest)

Starting with 1.50.0, these require Java 11.

Maintenance branches
--------------------
We currently support one maintenance branch:

* [Oak 1.22](https://github.com/apache/jackrabbit-oak/tree/1.22/) ([download](https://jackrabbit.apache.org/jcr/downloads.html#oak1.22)) - from Java 8 up to Java 13

End of life
-----------
The following branches have reached "end of life":

* [Oak 1.10](https://github.com/apache/jackrabbit-oak/tree/1.10/)
* [Oak 1.8](https://github.com/apache/jackrabbit-oak/tree/1.8/)
* [Oak 1.6](https://github.com/apache/jackrabbit-oak/tree/1.6/)
* [Oak 1.4](https://github.com/apache/jackrabbit-oak/tree/1.4/)
* [Oak 1.2](https://github.com/apache/jackrabbit-oak/tree/1.2/)
* [Oak 1.0](https://github.com/apache/jackrabbit-oak/tree/1.0/)

History
-------
~~~
                2014-12 +---------------------------------------------------------------\ 1.0
                2015-12 +------------------------------------------------------\ 1.2    | 1.0.25
                2016-12 +---------------------------------------------\ 1.4    | 1.2.22 | 1.0.35
                2017-12 +------------------------------------\ 1.6    | 1.4.18 | 1.2.27 | 1.0.39
   min JDK 8 -> 2018-03 +---------------------------\ 1.8    | 1.6.1  | 1.4.21 | 1.2.29 | 1.0.42
                2018-06 |                           | 1.8.4  | 1.6.12 | 1.4.22 |        |
                2018-09 |                           | 1.8.8  | 1.6.14 | 1.4.23 | 1.2.30 |
                2018-12 |                           | 1.8.10 | 1.6.15 |        |        |
                2019-03 +------------------\ 1.10   | 1.8.13 | 1.6.16 | 1.4.24 | 1.2.31 \ EOL
                2019-06 |                  | 1.10.3 | 1.8.14 | 1.6.17 |        |
                2019-09 |                  | 1.10.5 | 1.8.17 |        |        |
                2019-12 |                  | 1.10.7 | 1.8.18 | 1.6.18 |        |
                2020-01 +--------\ 1.22    | 1.10.8 | 1.8.19 | 1.6.19 | 1.4.25 |
 JDK 14 comp -> 2020-03 +-+ 1.26 | 1.22.2  | 1.10.8 | 1.8.21 | 1.6.20 | 1.4.26 |
                2020-06 +-+ 1.30 | 1.22.3  \ EOL    | 1.8.22 |        |        |
                2020-09 +-+ 1.34 | 1.22.4           | 1.8.23 |        |        |
                2020-12 +-+ 1.36 | 1.22.5           | 1.8.24 |        |        \ EOL
                2021-03 +-+ 1.38 | 1.22.6           |        |        |
                2021-06 +-+ 1.40 | 1.22.7           |        | 1.6.21 \ EOL
                2021-09 |        | 1.22.8           |        |
                2021-12 |        | 1.22.9           | 1.8.25 | 1.6.22
                2022-03 +-+ 1.42 | 1.22.11          | 1.8.26 | 1.6.23
                2022-06 |        |                  \EOL     \ EOL
                2022-09 +-+ 1.44 | 1.22.12
                2022-12 +-+ 1.46 | 1.22.13
  min JDK 11 -> 2023-03 +-+ 1.50 | 1.22.14
                2023-06 +-+ 1.52 | 1.22.15
Guava shaded -> 2023-09 +-+ 1.56 | 1.22.17
                2023-12 +-+ 1.60 | 1.22.18
                2024-03 +-+ 1.60 | 1.22.19
                2024-06 +-+ 1.64 | 1.22.20
~~~

The comments on the left side refer to backwards-incompatible changes, which,
by default, are not ported to maintenance branches.

"JDK 14 comp" refers to issue [OAK-7358](https://issues.apache.org/jira/browse/OAK-7358),
which was necessary to keep compatiblity with JDK 14.

"Guava shaded" refers to issue [OAK-10323](https://issues.apache.org/jira/browse/OAK-10323),
which was necessary to remove all API usages of the Guava library (Oak now internally
uses a shaded copy of Guava, which is not visible in Oak APIs anymore).
