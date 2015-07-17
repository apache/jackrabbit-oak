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

Password History
--------------------------------------------------------------------------------

### General

Oak provides functionality to remember a configurable number of
passwords after password changes and to prevent a password to
be set during changing a user's password if found in said history.

### Configuration

An administrator may enable password history via the
_org.apache.jackrabbit.oak.security.user.UserConfigurationImpl_
OSGi configuration. By default the history is disabled.

The following configuration option is supported:

- Maximum Password History Size (_passwordHistorySize_, number of passwords): When greater 0 enables password
  history and sets feature to remember the specified number of passwords for a user.

Note, that the current implementation has a limit of at most 1000 passwords
remembered in the history.

### How it works

#### Recording of Passwords

If the feature is enabled, during a user changing her password, the old password
hash is recorded in the password history.

The old password hash is only recorded if a password was set (non-empty).
Therefore setting a password for a user for the first time (i.e. during creation
or if the user doesn't have a password set before) does not result in a history
record, as there is no old password.

The old password hash is copied to the password history *after* the provided new
password has been validated but *before* the new password hash is written to the
user's _rep:password_ property.

The history operates as a FIFO list. A new password history record exceeding the
configured max history size, results in the oldest recorded password from being
removed from the history.

Also, if the configuration parameter for the history size is changed to a non-zero
but smaller value than before, upon the next password change the oldest records
exceeding the new history size are removed.

History password hashes are recorded in a multi-value property _rep:pwdHistory_ on
the user's _rep:pwd_ node.
        
The _rep:pwdHistory_ property is defined protected in order to guard against the 
user modifying (overcoming) her password history limitations.


#### Evaluation of Password History

Upon a user changing her password and if the password history feature is enabled
(configured password history size > 0), implementation checks if the current
password or  any of the password hashes recorded in the history matches the new
password.

If any record is a match, a _ConstraintViolationException_ is thrown and the
user's password is *NOT* changed.

#### Oak JCR XML Import

When users are imported via the Oak JCR XML importer, password history is imported
irrespective on whether the password history feature is enabled or not.
