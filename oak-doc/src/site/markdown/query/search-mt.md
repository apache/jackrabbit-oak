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

## Machine Translation for Search

* [Query time MT for Lucene indexes](#qtmtl)
    * [Apache Joshua](#joshua)
        * [Language Packs](#languagepacks)
    * [Setup](#setup)
        
Oak supports CLIR (Cross Language Information Retrieval) by using _Machine Translation_ to decorate search queries.
Such an extension is provided within the _oak-search-mt_ bundle.

### <a name="qtmtl"></a> Query time MT for Lucene indexes

Machine translation at query time is supported for Oak Lucene indexes by an extension of Oak Lucene's 
*FulltextQueryTermsProvider* API called *MTFulltextQueryTermsProvider*.
The initial implementation details can be found in [OAK-4348](https://issues.apache.org/jira/browse/OAK-4348).

The *MTFulltextQueryTermsProvider* will take the text of a given query and eventually translate it and provide a new 
Lucene query (to be added to the original one).
Query time machine translation will be performed in the *MTFulltextQueryTermsProvider* only if the index definition of the 
selected index matches the node types defined in the *MTFulltextQueryTermsProvider* configuration (e.g. Oak:Unstructured).

The *MTFulltextQueryTermsProvider* will try to perform the translation of the whole text first and, secondly, of the single 
tokens as they are created by the Lucene _Analyzer_ passed in the *#getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition)* 
API call.

Machine Translation is currently implemented by means of Apache Joshua, a statistical machine translation toolkit.
*MTFulltextQueryTermsProvider* will require a *language pack* (a SMT model) in order to perform translation of search queries.

#### <a name="joshua"></a> Apache Joshua

Apache Joshua is a statistical machine translation toolkit originally developed at Johns Hopkins University University of 
Pennsylvania, donated in 2015 to the Apache Software Foundation.
For more information on the usage of Apache Joshua for multi language search see the slides/video from the Berlin Buzzwords 2017 
presentation [Embracing diversity: searching over multiple languages](https://berlinbuzzwords.de/17/session/embracing-diversity-searching-over-multiple-languages).

##### <a name="languagepacks"></a> Language Packs

Apache Joshua can be used to train machine translation models called _language packs_, however it provides a set 
of ready to use (Apache licensed) language packs for many language pairs at:

[https://cwiki.apache.org/confluence/display/JOSHUA/Language+Packs](https://cwiki.apache.org/confluence/display/JOSHUA/Language+Packs)

#### <a name="setup"></a> Setup

Multiple *MTFulltextQueryTermsProvider* can be configured (for different language pairs) by using *MTFulltextQueryTermsProviderFactory* 
OSGi configuration factory.
In order to instantiate a *MTFulltextQueryTermsProviderFactory* the following properties need to be configured:
  
  * _path.to.config_ -> the path to the _joshua.config_ configuration file (e.g. of a downloaded language pack)
  * _node.types_ -> the list of node types for which query time MT expansion should be done
  * _min.score_ -> the minimum score (between 0 and 1) for a translated sentence / token to be used while expanding the query (this is used to filter out low quality translations)
  
