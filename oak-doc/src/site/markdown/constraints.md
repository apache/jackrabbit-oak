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

# Constraints

## Item Names

The JCR specification already restricts what can go into the "local" part of an
item name - see [JCR v2.0 Specification, Section 3.2](https://docs.adobe.com/docs/en/spec/jcr/2.0/3_Repository_Model.html#3.2%20Names).
In particular:

- Characters not allowed in XML are forbidden in names as well; this affects
  most control characters plus unpaired surrogates; see [Extensible Markup
  Language (XML) 1.0 (Fifth Edition), Section 2.2](https://www.w3.org/TR/xml/#NT-Char).
- Furthermore, the names `.` and `..` can not be used.
- Finally, the characters `/`,  `:`, `[`, `]`, `|`, and `*` are forbidden. For
these, the [JCR v2.0 Specification, Section 3.2.5.4](https://docs.adobe.com/docs/en/spec/jcr/2.0/3_Repository_Model.html#3.2.5.4%20Exposing%20Non-JCR%20Names) proposes a mapping to "private-use" code points.

On top of that, Oak implements several *additional* restrictions (as per
[JCR v2.0 Specification, Section 3.2.4](https://docs.adobe.com/content/docs/en/spec/jcr/2.0/3_Repository_Model.html#3.2.4%20Naming%20Restrictions)):

- The space character (U+0020) is disallowed at the beginning and the end of
a (local) name (see [JCR v2.0 Specification, Section 5.2.2.1](https://docs.adobe.com/content/docs/en/spec/jcr/2.0/5_Reading.html#5.2.2.1%20Name%20Patterns) for motivation).
- Other ASCII whitespace characters (CR, LF, TAB) are always disallowed (before OAK 1.10, more were disallowed, see [OAK-4857](https://issues.apache.org/jira/browse/OAK-4857)).

Finally, the chosen persistence implementation might restrict node names even further, for instance:

- in the "Document NodeStore", the UTF-8 representation of local names can not exceed ~150 bytes.

