Oak PojoSR
==========

This module demonstrates running Oak outside of OSGi environments but using in built OSGi
support for configuring Oak. It makes use of [PojoSR][1] to provide the OSGi framework
support. This would enable usage of Oak in POJO env and would still enable usage of OSGi features
to customize Oak components.

To make use of this following dependencies are required

1. PojoSR - Provides the OSGi framework support
2. Apache Felix SCR
3. Apache Felix Config Admin
4. Apache Felix Fileinstall - To provision configuration

[1]: https://code.google.com/p/pojosr/

License
-------

(see the top-level [LICENSE.txt](../LICENSE.txt) for full license details)

Collective work: Copyright 2012 The Apache Software Foundation.

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
