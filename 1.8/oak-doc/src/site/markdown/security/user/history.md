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

Since version 1.3.3 Oak provides functionality to remember a configurable number 
of passwords after password changes and to prevent a password to
be set during changing a user's password if found in said history.

### Configuration

An administrator may enable password history via the
`org.apache.jackrabbit.oak.security.user.UserConfigurationImpl`
OSGi configuration. By default the history is disabled (`passwordHistorySize` set to 0).

The following configuration option is supported:

| Parameter                     | Type    | Default  | Description        |
|-------------------------------|---------|----------|--------------------|
| `PARAM_PASSWORD_HISTORY_SIZE` | int     | 0        | Number of passwords to be stored in the history |
|  |  |  |  |

Setting the configuration option to a value greater than 0 enables password
history and sets feature to remember the specified number of passwords for a user.
Note, that the current implementation has a limit of at most 1000 passwords
remembered in the history.

### How it works

#### Representation in the Repository

History password hashes are recorded in a multi-value property `rep:pwdHistory` on
the user's `rep:pwd` node, which mandates the specific node type `rep:Password`
        
The `rep:pwdHistory` property is defined protected in order to guard against the 
user modifying (overcoming) her password history limitations.

    [rep:User]  > rep:Authorizable, rep:Impersonatable
        + rep:pwd (rep:Password) = rep:Password protected
        - rep:password (STRING) protected
        ...
        
    [rep:Password]
        - * (UNDEFINED) protected
        - * (UNDEFINED) protected multiple

#### Recording of Passwords

If the feature is enabled, during a user changing her password, the old password
hash is recorded in the password history.

The old password hash is only recorded if a password was set (non-empty).
Therefore setting a password for a user for the first time (i.e. during creation
or if the user doesn't have a password set before) does not result in a history
record, as there is no old password.

The old password hash is copied to the password history *after* the provided new
password has been validated but *before* the new password hash is written to the
user's `rep:password` property.

The history operates as a FIFO list. A new password history record exceeding the
configured max history size, results in the oldest recorded password from being
removed from the history.

Also, if the configuration parameter for the history size is changed to a non-zero
but smaller value than before, upon the next password change the oldest records
exceeding the new history size are removed.       

#### Evaluation of Password History

Upon a user changing her password and if the password history feature is enabled
(configured password history size > 0), implementation checks if the current
password or  any of the password hashes recorded in the history matches the new
password.

If any record is a match, a `ConstraintViolationException` is thrown and the
user's password is *NOT* changed.

#### XML Import

When users are imported via the JCR XML importer, password history is imported
irrespective on whether the password history feature is enabled or not.
