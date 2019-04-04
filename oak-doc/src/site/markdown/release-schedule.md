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

# Releases

## Schedule

We *aim* at keeping this frequency for releses. However dates may slip 
according to needs

- trunk: every 8 weeks (~6 / year)
- 1.10: every 4 weeks (~12 / year)
- 1.8: every 8 weeks (~6 / year)
- 1.6: every 13 weeks (~4 / year)
- 1.4: every 26 weeks (~2 / year)
- 1.2: every 52 weeks (1 / year)
- 1.0: every 52 weeks (1 / year)

## Strategies

For a full discussion around these topics see in [oak-dev archives](https://lists.apache.org/thread.html/9a7c0e2fdfab5deb051fbd99add6c2b7109d750805b6182138eece55@%3Coak-dev.jackrabbit.apache.org%3E).

- trunk will be considered stable
- only releases from trunk other than existing branches
- any previous release from trunk will be automatically deprecated

## Branching

Branching will not happen other than in specific circumstances. Such as,
but not limited to:

- incompatible API changes
- incompatible JVM changes
- updates to dependencies that breaks backward compatibility

In short: most probably it will always be around non-backward-compatible
changes

Anyhow in such cases the branching is not automatic and will be
discussed between PMCs a best course of actions. Alternatives may be a
different way to implement something breaking.

## Version Numbers

*Version Numbers*

- Released versions will be in the format of `Major.Minor.Patch` where,
as rule of thumb we will increase


    MAJOR for incompatible API changes    
    MINOR for new backwards-compatible functionality
    PATCH for backwards-compatible bug fixes.

- We'll keep the even/odd schema
- Any new official release will be always even: 1.12.0, 1.14.0, 1.16.0,
..., 1.124.0
- A release will always be with a patch number (the last part) of `.0`.
This ease OSGi deployments.
- Diagnostic builds will be cut with the odd version and `-Rxxx` such as
`1.15-R12345`.
- In case of branching the increased part will always be the PATCH so:
`1.16.0`, `1.16.1`, `1.16.2`, etc.
- In case of branching the diagnostic build will follow the current
pattern: `1.16.5-R12345`
