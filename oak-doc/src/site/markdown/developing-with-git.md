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

# Developing with Git

The Oak code base is backed by Git. It has its remote repository both at [ASF Gitbox](https://gitbox.apache.org/repos/asf/jackrabbit-oak.git) and [GitHub](https://github.com/apache/jackrabbit-oak).

For *committers* [write access to GitBox](https://infra.apache.org/git-primer.html) requires the Apache LDAP credentials via HTTPS Basic Authentication. For write access to GitHub you need to link your accounts once via the [GitBox Account Linking Utility](https://gitbox.apache.org/setup/).
For GitHub it is recommended to work with [SSH](https://docs.github.com/en/github/authenticating-to-github/connecting-to-github-with-ssh) instead of HTTPS authentication.

For *non-committers* it is recommended to use in your own forked GitHub repository and create PR from branches there. Further details in <https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork>