---
name: oak-store-document-add-config
description: Add a new OSGi configuration property to the oak-store-document module. Modifies Configuration.java, DocumentNodeStoreService.java, DocumentNodeStoreBuilder.java, RDBDocumentNodeStoreBuilder.java, Utils.java, and all relevant test files with 90%+ coverage.
allowed-tools: Read, Edit, Grep, Glob, Bash
disable-model-invocation: true
---

# oak-add-config Skill

You are an expert in the Apache Jackrabbit Oak `oak-store-document` module. When this skill is invoked, you will add a new OSGi configuration property.

## Supporting files

- Detailed production code patterns (steps 2–6): [osgi-config-patterns.md](osgi-config-patterns.md)
- Detailed test patterns (steps 7–10): [test-patterns.md](test-patterns.md)
- Wiring into usage class + compile/test (steps 11–12): [wiring-and-verification.md](wiring-and-verification.md)

---

## REQUIRED INPUT

The user **must** provide these values when invoking the skill. If any are missing, stop and ask:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `configName` | camelCase OSGi attribute method name | `avoidExclusiveMergeLock` |
| `type` | Java type: `boolean`, `int`, `long`, `String` | `boolean` |

`defaultValue` is **optional** — if not provided, use the Java default for the given type:
- `boolean` → `false`
- `int` / `long` → `0`
- `String` → `""`

The following are **optional** — derive or infer them if not provided:

| Parameter | Description | How to derive if missing |
|-----------|-------------|--------------------------|
| `jiraId` | JIRA ticket ID | **Deduce from current branch name** (see below) |
| `attrName` | Human-readable OSGi display name | Convert `configName` from camelCase to Title Case |
| `description` | Full OSGi description text | Ask the user for a brief description, then expand it |
| `scope` | `both`, `mongo-only`, or `rdb-only` | **Ask the user explicitly. Default is `mongo-only`.** |
| `featureToggle` | Whether to add a feature toggle (`true` / `false`) | **Ask the user explicitly. Default is `false`.** |

> **Deducing `jiraId` from the branch:**
> Run `git branch --show-current` and extract the JIRA ID
> (e.g. `OAK-12139` → `jiraId=OAK-12139`; `issue/OAK-12139` → `jiraId=OAK-12139`).
> If it differs from a user-supplied value, warn. If none can be deduced, ask.

> **Before proceeding, ask both optional questions in a single prompt:**
> 1. *"Which backends does this config apply to? (mongo-only / rdb-only / both, default: mongo-only)"*
> 2. *"Do you want a feature toggle for this config? (yes/no, default: no)"*

---

## DERIVED IDENTIFIERS

From the inputs, derive:

- `DEFAULT_CONST` → `DEFAULT_` + UPPER_SNAKE_CASE of `configName`
  e.g. `avoidExclusiveMergeLock` → `DEFAULT_AVOID_EXCLUSIVE_MERGE_LOCK`
- `setterName` → `set` + PascalCase(configName)
- `getterName` → same as `configName`
- `utilsMethod` → `is` + PascalCase(configName stripped of `avoid`/`enable`/`use`) + `Enabled`
  e.g. `isAvoidMergeLockEnabled`

**featureToggle=true only:**
- `FT_NAME_CONST` → `FT_NAME_` + UPPER_SNAKE_CASE of configName
- `FT_VALUE` → `"FT_"` + UPPER_SNAKE + `"_"` + jiraId
- `featureField` → `docStore` + PascalCase(configName stripped of trailing `Enabled`) + `Feature`

---

## EXECUTION STEPS

Work through every step in order. Do NOT skip any step.

Read the supporting files as you reach each group of steps.

1. **Read key files** — locate insertion points in all 9 files (listed below)
2. **Configuration.java** — add `@AttributeDefinition` + import → see [osgi-config-patterns.md](osgi-config-patterns.md)
3. **DocumentNodeStoreService.java** — DEFAULT constant, FT_NAME, Feature field, activate/deactivate, configureBuilder → see [osgi-config-patterns.md](osgi-config-patterns.md)
4. **DocumentNodeStoreBuilder.java** — value field, getter/setter, Feature getter/setter → see [osgi-config-patterns.md](osgi-config-patterns.md)
5. **RDBDocumentNodeStoreBuilder.java** — override to disable (scope=mongo-only) → see [osgi-config-patterns.md](osgi-config-patterns.md)
6. **Utils.java** — add `isXxxEnabled()` method → see [osgi-config-patterns.md](osgi-config-patterns.md)
7. **DocumentNodeStoreServiceConfigurationTest.java** → see [test-patterns.md](test-patterns.md)
8. **MongoDocumentNodeStoreBuilderTest.java** → see [test-patterns.md](test-patterns.md)
9. **RDBDocumentNodeStoreBuilderTest.java** → see [test-patterns.md](test-patterns.md)
10. **UtilsTest.java** → see [test-patterns.md](test-patterns.md)
11. **Wire into usage class** → see [wiring-and-verification.md](wiring-and-verification.md)
12. **Compile and run tests** → see [wiring-and-verification.md](wiring-and-verification.md)

### Files to read in STEP 1

```
oak-store-document/src/main/java/org/apache/jackrabbit/oak/plugins/document/Configuration.java
oak-store-document/src/main/java/org/apache/jackrabbit/oak/plugins/document/DocumentNodeStoreService.java
oak-store-document/src/main/java/org/apache/jackrabbit/oak/plugins/document/DocumentNodeStoreBuilder.java
oak-store-document/src/main/java/org/apache/jackrabbit/oak/plugins/document/rdb/RDBDocumentNodeStoreBuilder.java
oak-store-document/src/main/java/org/apache/jackrabbit/oak/plugins/document/util/Utils.java
oak-store-document/src/test/java/org/apache/jackrabbit/oak/plugins/document/DocumentNodeStoreServiceConfigurationTest.java
oak-store-document/src/test/java/org/apache/jackrabbit/oak/plugins/document/mongo/MongoDocumentNodeStoreBuilderTest.java
oak-store-document/src/test/java/org/apache/jackrabbit/oak/plugins/document/rdb/RDBDocumentNodeStoreBuilderTest.java
oak-store-document/src/test/java/org/apache/jackrabbit/oak/plugins/document/util/UtilsTest.java
```

---

## QUALITY CHECKLIST

Before finishing, verify:

- [ ] `Configuration.java` has a new `@AttributeDefinition` entry with correct default import
- [ ] `DocumentNodeStoreService.java` has DEFAULT constant and configureBuilder value setter
- [ ] _(featureToggle=true)_ `DocumentNodeStoreService.java` has FT_NAME constant, Feature field, activate registration, deactivate cleanup, and feature setter in configureBuilder
- [ ] `DocumentNodeStoreBuilder.java` has value field + getter/setter pair
- [ ] _(featureToggle=true)_ `DocumentNodeStoreBuilder.java` has Feature field + Feature getter/setter pair
- [ ] `RDBDocumentNodeStoreBuilder.java` overrides value getter/setter to disable (scope=mongo-only)
- [ ] _(featureToggle=true)_ `RDBDocumentNodeStoreBuilder.java` also overrides Feature getter/setter (scope=mongo-only)
- [ ] `Utils.java` has `isXxxEnabled()` with proper javadoc
- [ ] `DocumentNodeStoreServiceConfigurationTest.java` tests default value + override
- [ ] `MongoDocumentNodeStoreBuilderTest.java` tests default getter (+ null feature if featureToggle=true)
- [ ] `RDBDocumentNodeStoreBuilderTest.java` has symmetrical default-value tests (scope=both) or override-disabled tests (scope=mongo-only)
- [ ] `UtilsTest.java` has correct number of tests for featureToggle + scope combination
- [ ] `DocumentNodeStore.java` reads the config via `{utilsMethod}(builder)` and stores it as a field
- [ ] Target class receives and uses the field correctly
- [ ] All tests pass
- [ ] No new static imports added to any test file

---

## IMPORTANT CONSTRAINTS

- **Assertion style**: match the existing imports in each file — do NOT add new imports. If the file uses `assertFalse(...)` (static), use that; if it uses `Assert.assertFalse(...)`, use that.
- **No static imports in new production code**
- **JUnit 4** only (`@Test` from `org.junit.Test`)
- **Apply standards only to new code** — never reformat or rename existing code
- Follow the exact naming and placement patterns shown in the supporting files
