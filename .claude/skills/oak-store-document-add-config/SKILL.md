---
name: oak-store-document-add-config
description: Add a new OSGi configuration property to the oak-store-document module. Modifies Configuration.java, DocumentNodeStoreService.java, DocumentNodeStoreBuilder.java, RDBDocumentNodeStoreBuilder.java, Utils.java, and all relevant test files with 90%+ coverage.
tools: Read, Edit, Grep, Glob, Bash
---

# oak-add-config Skill

You are an expert in the Apache Jackrabbit Oak `oak-store-document` module. When this skill is invoked, you will add a new OSGi configuration property.

## REQUIRED INPUT

The user **must** provide these two values when invoking the skill. If any are missing, stop and ask before proceeding:

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
> Run `git branch --show-current` and extract the JIRA ID from the branch name
> (e.g. branch `OAK-12139` → `jiraId=OAK-12139`; branch `issue/OAK-12139` → `jiraId=OAK-12139`).
> - If the user explicitly provided a `jiraId` that **differs** from the branch, use the user-provided value
>   but warn: *"Note: using jiraId {user-provided} but current branch is {branch-name}."*
> - If no `jiraId` can be deduced and the user did not provide one, ask for it.

> **Before proceeding, ask both questions in a single prompt:**
>
> 1. *"Which backends does this config apply to? (mongo-only / rdb-only / both, default: mongo-only)"*
> 2. *"Do you want a feature toggle for this config? (yes/no, default: no)"*
>
> Accept the defaults silently if the user just presses enter or says nothing specific.

From those inputs, **derive** the following identifiers:

- `DEFAULT_CONST` → `DEFAULT_` + UPPER_SNAKE_CASE of `configName`
  e.g. `avoidExclusiveMergeLock` → `DEFAULT_AVOID_EXCLUSIVE_MERGE_LOCK`
- `setterName` → `set` + PascalCase(configName)
  e.g. `setAvoidMergeLock`
- `getterName` → same as `configName` (returns the raw boolean/value)
  e.g. `avoidMergeLock`
- `utilsMethod` → `is` + PascalCase(configName stripped of `avoid`/`enable`/`use`, then add `Enabled`)
  e.g. `isAvoidMergeLockEnabled`
**Only when `featureToggle=true`**, also derive:
- `FT_NAME_CONST` → `FT_NAME_` + UPPER_SNAKE_CASE of configName (replacing camel humps with `_`)
  e.g. `FT_NAME_AVOID_MERGE_LOCK` (shortened, business-meaningful)
- `FT_VALUE` → `"FT_"` + UPPER_SNAKE + `"_"` + jiraId
  e.g. `"FT_AVOID_MERGE_LOCK_OAK-12345"`
- `featureField` → `docStore` + PascalCase(configName stripped of trailing `Enabled`) + `Feature`
  e.g. `docStoreAvoidMergeLockFeature`

---

## EXECUTION STEPS

Work through every step in order. Do NOT skip any step.

---

### STEP 1 — Read the key files to locate insertion points

Read these files and identify WHERE each change goes:

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

### STEP 2 — Configuration.java

Add an `@AttributeDefinition` entry at the **end of the interface**, just before the closing `}`.

Pattern to follow:
```java
@AttributeDefinition(
        name = "{attrName}",
        description = "{description}. The Default value is " + {DEFAULT_CONST} +
                " Note that this value can be overridden via framework property 'oak.documentstore.{configName}'")
{type} {configName}() default {DEFAULT_CONST};
```

Also add the import for `DEFAULT_CONST` from `DocumentNodeStoreService`:
```java
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.{DEFAULT_CONST};
```
(add it alphabetically alongside the other static imports at the top of Configuration.java)

---

### STEP 3 — DocumentNodeStoreService.java

#### 3a. Add the DEFAULT constant
Find the block of `static final boolean DEFAULT_*` constants (near line 170) and add:
```java
static final {type} {DEFAULT_CONST} = {defaultValue};
```
Add it in alphabetical order among the similar constants.

#### 3b. Add the FT_NAME constant _(only when `featureToggle=true`)_
Find the block of `private static final String FT_NAME_*` constants (near line 200) and add:
```java
/**
 * Feature toggle name to {shortened description for javadoc}
 */
private static final String {FT_NAME_CONST} = "{FT_VALUE}";
```

#### 3c. Add the Feature field _(only when `featureToggle=true`)_
Find the block of `private Feature *` fields (near line 263) and add:
```java
private Feature {featureField};
```

#### 3d. Register the Feature in `activate()` _(only when `featureToggle=true`)_
Find the `activate()` method and add alongside the other `Feature.newFeature(...)` calls:
```java
{featureField} = Feature.newFeature({FT_NAME_CONST}, whiteboard);
```

#### 3e. Close the Feature in `deactivate()` _(only when `featureToggle=true`)_
Find the `closeFeatures(...)` call and add `{featureField}` to the argument list.

#### 3f. Configure the builder in `configureBuilder()`
Find the method chain in `configureBuilder()` and add the config value setter:
```java
.set{PascalCase(configName)}(config.{configName}())
```
When `featureToggle=true`, also add the feature setter immediately after:
```java
.setDocStore{PascalCase(featureField minus docStore prefix and Feature suffix)}Feature({featureField})
```
(Adjacent to similar ones)

---

### STEP 4 — DocumentNodeStoreBuilder.java

#### 4a. Feature field _(only when `featureToggle=true`)_
Find the block of `private Feature *` fields and add:
```java
private Feature {featureField};
```

#### 4b. Boolean/value field
Find the block of `private boolean *` or `private {type} *` fields and add:
```java
private {type} {getterName};
```

#### 4c. Getter + Setter for the value
Add adjacent to similar pairs (e.g., next to `setThrottlingEnabled` / `isThrottlingEnabled`):
```java
public T {setterName}({type} b) {
    this.{getterName} = b;
    return thisBuilder();
}

public {type} {getterName}() {
    return this.{getterName};
}
```

#### 4d. Getter + Setter for the Feature _(only when `featureToggle=true`)_
Add adjacent to similar Feature pairs:
```java
public Feature get{PascalCase(featureField)}() {
    return {featureField};
}

public T set{PascalCase(featureField)}(@Nullable Feature {featureField}) {
    this.{featureField} = {featureField};
    return thisBuilder();
}
```

---

### STEP 5 — RDBDocumentNodeStoreBuilder.java (only if scope is `both` or `mongo-only`)

When scope is `mongo-only`: RDB **does NOT support** the config. Override to disable it.

Always override the value getter/setter:
```java
@Override
public RDBDocumentNodeStoreBuilder {setterName}({type} b) {
    return thisBuilder();
}

@Override
public {type} {getterName}() {
    // setting this is not supported for RDB
    return {disabledValue};   // false for boolean, 0 for int/long, null for String
}
```

When `featureToggle=true`, also override the Feature getter/setter:
```java
@Override
public RDBDocumentNodeStoreBuilder set{PascalCase(featureField)}(@Nullable Feature f) {
    return thisBuilder();
}

@Override
@Nullable
public Feature get{PascalCase(featureField)}() {
    return null;
}
```

When scope is `rdb-only`: Apply the same override pattern but in MongoDocumentNodeStoreBuilder instead.

When scope is `both`: No overrides needed — the base class implementation applies to both.

---

### STEP 6 — Utils.java

Add a new static utility method near the other `isXxxEnabled()` methods (e.g., next to `isThrottlingEnabled`, `isFullGCEnabled`, `isEmbeddedVerificationEnabled`):

**When `featureToggle=true`:**
```java
/**
 * Check whether {attrName} is enabled or not for document store.
 *
 * @param builder instance for DocumentNodeStoreBuilder
 * @return true if {attrName} is enabled else false
 */
public static boolean {utilsMethod}(final DocumentNodeStoreBuilder<?> builder) {
    final Feature {featureField} = builder.get{PascalCase(featureField)}();
    return builder.{getterName}() || ({featureField} != null && {featureField}.isEnabled());
}
```

**When `featureToggle=false`:**
```java
/**
 * Check whether {attrName} is enabled or not for document store.
 *
 * @param builder instance for DocumentNodeStoreBuilder
 * @return true if {attrName} is enabled else false
 */
public static boolean {utilsMethod}(final DocumentNodeStoreBuilder<?> builder) {
    return builder.{getterName}();
}
```

---

### STEP 7 — Tests: DocumentNodeStoreServiceConfigurationTest.java

Add **2 new test methods** using JUnit 4 style. Match the file's existing assertion import style (see IMPORTANT CONSTRAINTS).

#### Test 1: Default value check
Find the `defaultValues()` test and add an assertion line:
```java
Assert.assertEquals({DEFAULT_CONST}, config.{configName}());
```

#### Test 2: Config override test
Add a new test method:
```java
@Test
public void {configName}Enabled() throws Exception {
    {type} value = {nonDefaultValue};
    addConfigurationEntry(preset, "{configName}", value);
    Configuration config = createConfiguration();
    Assert.assertEquals(value, config.{configName}());
}
```
Also add the import:
```java
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.{DEFAULT_CONST};
```
(add it alphabetically)

---

### STEP 8 — Tests: MongoDocumentNodeStoreBuilderTest.java

Always add:
```java
@Test
public void {getterName}Disabled() {
    MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
    assertFalse(builder.{getterName}());
}
```

When `featureToggle=true`, also add:
```java
@Test
public void {getterName}FeatureToggleEnabled() {
    MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
    assertNull(builder.get{PascalCase(featureField)}());
}
```

---

### STEP 9 — Tests: RDBDocumentNodeStoreBuilderTest.java (only if scope is `both` or `mongo-only`)

**When scope is `both`** (RDB uses the base class — no overrides): add the same symmetrical default-value tests as in step 8 to verify the base class behaviour works correctly for RDB too:
```java
@Test
public void {getterName}Disabled() {
    RDBDocumentNodeStoreBuilder builder = new RDBDocumentNodeStoreBuilder();
    assertFalse(builder.{getterName}());
}
```

When `featureToggle=true`, also add:
```java
@Test
public void {getterName}FeatureToggleEnabled() {
    RDBDocumentNodeStoreBuilder builder = new RDBDocumentNodeStoreBuilder();
    assertNull(builder.get{PascalCase(featureField)}());
}
```

**When scope is `mongo-only`** (RDB overrides return disabled/null), always add:
```java
@Test
public void {getterName}Disabled() {
    RDBDocumentNodeStoreBuilder builder = new RDBDocumentNodeStoreBuilder();
    builder.{setterName}({nonDefaultValue});
    assertFalse(builder.{getterName}());
}
```

When `featureToggle=true`, also add:
```java
@Test
public void {getterName}FeatureToggleDisabled() {
    RDBDocumentNodeStoreBuilder builder = new RDBDocumentNodeStoreBuilder();
    Feature feature = mock(Feature.class);
    when(feature.isEnabled()).thenReturn(true);
    builder.set{PascalCase(featureField)}(feature);
    assertNull(builder.get{PascalCase(featureField)}());
}
```

> **Note on assertion style**: `RDBDocumentNodeStoreBuilderTest.java` uses static imports for assertions and Mockito (`assertFalse`, `assertNull`, `mock`, `when`). Match this style.

---

### STEP 10 — Tests: UtilsTest.java

> **Important**: `UtilsTest.java` uses static imports for assertions (`assertFalse`, `assertTrue`, etc.). Check the existing imports at the top of the file and use whatever assertion/Mockito style is already present. Do NOT add new imports.

These tests use `newDocumentNodeStoreBuilder()` and `newRDBDocumentNodeStoreBuilder()` (already imported).

**Always add** (applies regardless of `featureToggle`):
```java
@Test
public void {getterName}DefaultValue() {
    boolean result = Utils.{utilsMethod}(DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder());
    assertFalse("Feature is disabled by default", result);
}

@Test
public void {getterName}EnabledViaConfiguration() {
    DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder();
    builder.{setterName}(true);
    assertTrue("Feature is enabled via configuration", Utils.{utilsMethod}(builder));
}
```

**When `featureToggle=true`**, also add:
```java
@Test
public void {getterName}ExplicitlyDisabled() {
    DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder();
    builder.{setterName}(false);
    Feature feature = Mockito.mock(Feature.class);
    Mockito.when(feature.isEnabled()).thenReturn(false);
    builder.set{PascalCase(featureField)}(feature);
    assertFalse("Feature is disabled explicitly", Utils.{utilsMethod}(builder));
}

@Test
public void {getterName}EnabledViaFeatureToggle() {
    DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder();
    builder.{setterName}(false);
    Feature feature = Mockito.mock(Feature.class);
    Mockito.when(feature.isEnabled()).thenReturn(true);
    builder.set{PascalCase(featureField)}(feature);
    assertTrue("Feature is enabled via feature toggle", Utils.{utilsMethod}(builder));
}
```

**When scope is `mongo-only`** (skip if `rdb-only`), add:
```java
@Test
public void {getterName}DisabledForRDB() {
    DocumentNodeStoreBuilder<?> builder = RDBDocumentNodeStoreBuilder.newRDBDocumentNodeStoreBuilder();
    builder.{setterName}(true);
    assertFalse("Feature is disabled for RDB", Utils.{utilsMethod}(builder));
}
```

Total: 2 tests (no feature toggle, both scope) up to 5 tests (feature toggle + mongo-only).
- no featureToggle + both: 2
- no featureToggle + mongo-only: 3
- featureToggle + both: 4
- featureToggle + mongo-only: 5

---

### STEP 11 — Wire the config into its usage class

After all infrastructure changes are done, ask the user:

> **"Which class do you want to use `{utilsMethod}()` in? (e.g. `DocumentNodeStoreBranch`, `DocumentNodeStore`, etc.)"**

Wait for the answer before proceeding. Then wire it as follows:

#### Pattern (based on how `avoidMergeLock` was wired into `DocumentNodeStoreBranch`):

**11a. `DocumentNodeStore.java` — read config from builder, store as field**

Find the constructor `DocumentNodeStore(DocumentNodeStoreBuilder<?> builder)` and the existing block where similar fields are assigned (e.g. `this.avoidMergeLock = isAvoidMergeLockEnabled(builder)`). Add:
```java
this.{fieldName} = {utilsMethod}(builder);
```
Also add the corresponding `private final boolean {fieldName};` field declaration near the other similar fields.

Add the static import for `{utilsMethod}` alongside the other `Utils.*` static imports at the top of `DocumentNodeStore.java`.

**11b. Target class — pass the value through**

If the target class is `DocumentNodeStoreBranch` (or another class instantiated by `DocumentNodeStore`):

1. Find where `DocumentNodeStore` creates the target class (e.g. `createBranch(...)`) and add the field as an extra argument.
2. In the target class constructor, accept the new parameter and assign it to a field:
   ```java
   private final boolean {fieldName};
   // in constructor:
   this.{fieldName} = {fieldName};
   ```
3. Use `this.{fieldName}` wherever the logic needs it in the target class.

If the target class IS `DocumentNodeStore` itself, skip 11b — the field is already there from 11a.

**11c. Read the target class first**

Before making any changes, read `DocumentNodeStore.java` and the target class to understand the existing constructor signatures and field patterns. Match the style exactly.

**11d. Update ALL existing constructor call sites (STRICTLY ENFORCED)**

When adding a new parameter to a target class constructor:

- **NEVER add a backward-compatibility overload** (no delegate constructors that just pass `false`/`0`/`null`).
- Instead, **grep for all existing call sites** of the modified constructor (in both production code and test code) and update every one to pass the appropriate default value for the new parameter.

```bash
# Find all call sites before editing
grep -rn "new TargetClass(" oak-store-document/src/
```

For each call site found, add the new argument with its default value (e.g. `false` for boolean, `0` for int/long, `null` for String). This keeps the codebase clean and avoids accumulating stale overloads.

---

### STEP 12 — Compile and run tests

```bash
cd "$(git rev-parse --show-toplevel)"
mvn compile -pl oak-store-document -q 2>&1 | tail -20
```

If compilation fails, diagnose and fix, then retry.

```bash
mvn test -pl oak-store-document -am \
  -Dtest="DocumentNodeStoreServiceConfigurationTest,MongoDocumentNodeStoreBuilderTest,RDBDocumentNodeStoreBuilderTest,UtilsTest" \
  2>&1 | tail -40
```

Fix any test failures. Re-run until all pass.

> **Note**: Pre-existing unrelated compile errors in other classes (e.g. `NodeCache.java`) should be ignored — verify they exist on trunk before treating them as caused by your changes.

---

## QUALITY CHECKLIST

Before finishing, verify:

- [ ] `Configuration.java` has a new `@AttributeDefinition` entry with correct default import
- [ ] `DocumentNodeStoreService.java` has DEFAULT constant and configureBuilder value setter
- [ ] _(featureToggle=true)_ `DocumentNodeStoreService.java` has FT_NAME constant, Feature field, activate registration, deactivate cleanup, and feature setter in configureBuilder
- [ ] `DocumentNodeStoreBuilder.java` has value field + getter/setter pair
- [ ] _(featureToggle=true)_ `DocumentNodeStoreBuilder.java` has Feature field + Feature getter/setter pair
- [ ] `RDBDocumentNodeStoreBuilder.java` overrides value getter/setter to disable (if scope is mongo-only)
- [ ] _(featureToggle=true)_ `RDBDocumentNodeStoreBuilder.java` also overrides Feature getter/setter (if scope is mongo-only)
- [ ] `Utils.java` has `isXxxEnabled()` with proper javadoc (simple or feature-aware depending on featureToggle)
- [ ] `DocumentNodeStoreServiceConfigurationTest.java` tests default value + override
- [ ] `MongoDocumentNodeStoreBuilderTest.java` tests default getter (+ null feature if featureToggle=true)
- [ ] `RDBDocumentNodeStoreBuilderTest.java` has symmetrical default-value tests (scope=both) or override-disabled tests (scope=mongo-only)
- [ ] `UtilsTest.java` has correct number of tests for featureToggle + scope combination
- [ ] `DocumentNodeStore.java` reads the config via `{utilsMethod}(builder)` and stores it as a field
- [ ] Target class (named by user) receives and uses the field correctly
- [ ] All tests pass
- [ ] No new static imports added to any test file

## IMPORTANT CONSTRAINTS

- **Assertion style in test files**: always match the existing imports in each file — do NOT add new imports to impose a style. If the file uses `assertFalse(...)` (static import), use that. If it imports `org.junit.Assert`, use `Assert.assertFalse(...)`.
- **No static imports in new production code** (all test assertions follow file convention as above; Mockito calls: `Mockito.mock(...)`, `Mockito.when(...)`)
- **JUnit 4** only (`@Test` from `org.junit.Test`)
- **Apply standards only to new code** — never reformat or rename existing code
- Follow the exact naming and placement patterns shown in the steps above
