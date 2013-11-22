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

Oak Documentation
=================

The Oak documentation lives as Markdown files in `src/site/markdown` such
that it easy to view e.g. from GitHub. Alternatively the Maven site plugin
can be used to build and deploy a web site as follows:

From the reactor use

    mvn site

to build the site without Javadoc or

    mvn site -Psite-with-javadoc

to build the site with Javadoc. Review the site at `oak-doc/target/site`.

Then deploy the site to `http://jackrabbit.apache.org/oak/docs/` using

    mvn site-deploy

Finally review the site at `http://jackrabbit.apache.org/oak/docs/index.html`.
To skip the final commit use `-Dscmpublish.skipCheckin=true`. You can then
review all pending changes in `oak-doc/target/scmpublish-checkout` and follow
up with `svn commit` manually.

Every committer should be able to deploy the site. No fiddling with
credentials needed since deployment is done via svn commit to
`https://svn.apache.org/repos/asf/jackrabbit/site/live/oak/docs`.
