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

# Participating

## Mailing Lists

The best place for Oak-related discussions is the [oak-dev@](mailto:oak-dev@jackrabbit.apache.org)
mailing list. To subscribe, send a message to [oak-dev-subscribe@](mailto:oak-dev-subscribe@jackrabbit.apache.org).

For more details related to various mailing list have a look at http://jackrabbit.apache.org/mailing-lists.html.

## Issue Tracking

Use the [OAK issue tracker](https://issues.apache.org/jira/browse/OAK) to submit issues, comments 
or patches. To subscribe to issue notifications, send a message to
[oak-issues@](mailto:oak-issues-subscribe@jackrabbit.apache.org).

- On "trunk": when done with a ticket, set it to "resolved" and set "Fix Version"
  to the next unreleased version. Our workflow does not allow re-opening a
  closed ticket, so we close tickets only when a public release has been made
  with that change. Until that happens, a ticket can always be re-opened
  and further work can happen.
- On maintenance branch (currently 1.22): re-use the existing Jira ticket an
  just add to "Fix Version" (unless the backport is complex).
- Add "Affects Version" and "Fix Version" as and when applicable (but not
  otherwise).
- Be careful not to include sensitive information (be it in descriptions,
  attachments, or log files).
 
## Source Code

The latest Oak sources are available on [GitHub](https://github.com/apache/jackrabbit-oak).
To subscribe to commit notifications, send a message to [oak-commits@](mailto:oak-commits-subscribe@jackrabbit.apache.org).

### Making Changes

#### Pull Requests (PRs)

- Minimize PRs; do not modify whitespace/coding style except where needed. This
  makes them much easier to review, also minimizes confusion when using
  "git blame".
- Structure tickets/PRs so that things that can be separated are (that can be
  useful for backports and reverts).
- Have test cases (when there's no immediate fix, create a ticket and a PR just
  for the test and mark it "ignored", pointing to the actual issue).
- PRs that contain multiple commits in general should be "squashed and merged".
- When new files are added, make sure they have the proper license on it
  (in doubt, run the build with "-Prat").

#### Commits

- Always reference an Oak ticket for each commit/PR (this should include the JIRA id
  in the correct format, e.g. "OAK-10881" instead of "Oak 10881").
- Avoid committing unfinished stuff; in particular when a release is approaching
  (see [UNRELEASED](https://issues.apache.org/jira/projects/OAK?selectedItem=com.atlassian.jira.jira-projects-plugin%3Arelease-page&status=unreleased))

### Coding Style

- Please avoid wildcard imports.
- In general be consistent with the style of the code being modified.
- Avoid TABs, non-ASCII characters, and trailing whitespace.
