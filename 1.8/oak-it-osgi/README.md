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
Oak Integration tests for OSGi deployments
==

This modules validates basic Oak functionality in an OSGi environment.

The Oak modules under test are not picked up from the reactor build. Instead,
they are read from the local repository and copied in a well-known location
using the maven-assembly-plugin.

In order to test a change made in a different module, you would need to:

* Run `mvn install` in the changed module
* Run `mvn assembly:single` in this module
* Re-run the test(s) in this module
