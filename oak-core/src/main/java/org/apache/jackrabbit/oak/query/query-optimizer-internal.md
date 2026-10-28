# Query Optimizer (OAK-12489)

## Purpose

The Query Optimizer allows to rewrite
queries to work around query problem without changing the application.

It uses regex find/replace rules.
Rewriting happens before parsing, and, for XPath, before conversion to
SQL-2. The existing query planning and cost-based optimization still run
afterward.

## Enabling the feature

The feature is disabled by default. Either of the following enables it:

- JVM system property: `oak.query.optimizer=true`
- Feature toggle: `FT_QUERY_OPTIMIZER_OAK-12489`

E.g.

```text
java -Doak.query.optimizer=true cq-quickstart-6.6.0-SNAPSHOT.jar 
```

## Repository configuration

Add a `diff.index.optimizer` with a `queryRules.json` file:

```text
/oak:index/diff.index.optimizer
  jcr:primaryType = oak:QueryIndexDefinition
  type = disabled
/oak:index/diff.index.optimizer/queryRules.json
  nt:file
```

Example:

```text
[
  {
    "find": "\\A(\\s*(?i:explain\\s+)?(?i:measure\\s+)?)\\Q/jcr:root/content//element(*, cq:Page)[(jcr:contains(., '\\E((?:[^']++|'')*+)\\Q'))]\\E\\z",
    "replace": "$1/jcr:root/content//element(*, cq:Page)[(jcr:contains(., '$2'))] option(index tag pages)",
    "options": {
      "stop": true
    }
  }
]```

## Rule format

The file contains an array of objects. Each object supports:

| Field | Required | Meaning |
|---|---|---|
| `find` | Yes | Java regular expression applied to the statement. |
| `replace` | Yes | Java regex replacement string, including capture references such as `$1`. |
| `language` | No | Apply only to this query language. Omission means all languages. |
| `options` | No | An object containing supported rule options. |
| `options.stop` | No | Boolean; default `false`. Stop processing later rules after this rule matches. |

Use the language names `xpath`, `JCR-SQL2`, `sql`, or `JCR-JQOM`. Language matching
is case-sensitive.

The only option is `stop` currently.
There is no option to rewrite Oak-internal queries.

## Processing semantics

Rules run in array order. Each rule sees the output of the previous rule.

- A pattern may match part of the statement; full-statement matching is not
  implicit.
- All non-overlapping matches of a rule are replaced, as with Java
  `Matcher.replaceAll`.
- Each rule is visited once. Processing does not restart after a replacement.
- `stop=true` stops later rules only if the current rule matched.
- A matching rule that leaves the text unchanged still triggers `stop=true`.
- An unmatched or language-ineligible rule does not stop processing.

Internal Oak queries are excluded.

Existing length limits, query-validator checks, literal restrictions, and result
permission checks remain in effect. The query validator checks both the original
statement and a changed replacement statement.

## Example: select indexes tagged `pages`

This rule matches a particular XPath query shape and appends
`option(index tag pages)`. It preserves an optional `explain`, `measure`, or
`explain measure` prefix:

```json
[
  {
    "find": "\\A(\\s*(?i:explain\\s+)?(?i:measure\\s+)?)\\Q/jcr:root/content//element(*, cq:Page)[(jcr:contains(., '\\E((?:[^']++|'')*+)\\Q'))]\\E\\z",
    "replace": "$1/jcr:root/content//element(*, cq:Page)[(jcr:contains(., '$2'))] option(index tag pages)",
    "options": {
      "stop": true
    }
  }
]
```

An optional `"language": "xpath"` can be added to restrict the rule explicitly.

Input:

```text
/jcr:root/content//element(*, cq:Page)[(jcr:contains(., 'hello world'))]
```

Effective statement:

```text
/jcr:root/content//element(*, cq:Page)[(jcr:contains(., 'hello world'))] option(index tag pages)
```

The same transformation applies when the input starts with `explain ` or
`measure `. These prefixes are part of the submitted statement: the rewriter does
not remove or preserve them automatically.

In this pattern, `$1` preserves the prefix and `$2` preserves the escaped
full-text contents. `\A` and `\z` require the entire statement to match.

## Refresh and caching

While enabled, the rewriter checks the `jcr:lastModified` of the `queryRules.json` file once every **10 seconds**. 
There is no background polling thread.

## Failure handling and limits

The configuration supports at most **100 rules** and **1 MiB of UTF-8 file
content**.

## Diagnostics and troubleshooting

The current implementation logs changed statements at INFO through
`org.apache.jackrabbit.oak.query.QueryEngineImpl`:

```text
Rewriting <language> query: <original> to <effective>
```

Refresh failures are logged at WARN through
`org.apache.jackrabbit.oak.query.QueryOptimizer`:

```text
Cannot refresh query rules; keeping the last valid rules
```
