# Production Code Change Patterns

## STEP 2 — Configuration.java

Add an `@AttributeDefinition` entry at the **end of the interface**, just before the closing `}`.

```java
@AttributeDefinition(
        name = "{attrName}",
        description = "{description}. The Default value is " + {DEFAULT_CONST} +
                " Note that this value can be overridden via framework property 'oak.documentstore.{configName}'")
{type} {configName}() default {DEFAULT_CONST};
```

Also add the import for `DEFAULT_CONST` from `DocumentNodeStoreService` alphabetically:
```java
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.{DEFAULT_CONST};
```

---

## STEP 3 — DocumentNodeStoreService.java

#### 3a. DEFAULT constant
Find the `static final boolean DEFAULT_*` block (near line 170) and add in alphabetical order:
```java
static final {type} {DEFAULT_CONST} = {defaultValue};
```

#### 3b. FT_NAME constant _(featureToggle=true only)_
Find the `private static final String FT_NAME_*` block (near line 200):
```java
/**
 * Feature toggle name to {shortened description for javadoc}
 */
private static final String {FT_NAME_CONST} = "{FT_VALUE}";
```

#### 3c. Feature field _(featureToggle=true only)_
Find the `private Feature *` fields block (near line 263):
```java
private Feature {featureField};
```

#### 3d. Register Feature in `activate()` _(featureToggle=true only)_
```java
{featureField} = Feature.newFeature({FT_NAME_CONST}, whiteboard);
```

#### 3e. Close Feature in `deactivate()` _(featureToggle=true only)_
Add `{featureField}` to the `closeFeatures(...)` argument list.

#### 3f. Configure builder in `configureBuilder()`
```java
.set{PascalCase(configName)}(config.{configName}())
```
When `featureToggle=true`, also add immediately after:
```java
.setDocStore{PascalCase(featureField minus docStore prefix and Feature suffix)}Feature({featureField})
```

---

## STEP 4 — DocumentNodeStoreBuilder.java

#### 4a. Feature field _(featureToggle=true only)_
```java
private Feature {featureField};
```

#### 4b. Value field
```java
private {type} {getterName};
```

#### 4c. Getter + Setter for the value
```java
public T {setterName}({type} b) {
    this.{getterName} = b;
    return thisBuilder();
}

public {type} {getterName}() {
    return this.{getterName};
}
```

#### 4d. Getter + Setter for the Feature _(featureToggle=true only)_
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

## STEP 5 — RDBDocumentNodeStoreBuilder.java

**scope=mongo-only**: RDB does NOT support the config — override to disable:
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

When `featureToggle=true`, also override Feature getter/setter:
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

**scope=rdb-only**: Apply the same override pattern in `MongoDocumentNodeStoreBuilder` instead.

**scope=both**: No overrides needed — base class applies to both.

---

## STEP 6 — Utils.java

Add near the other `isXxxEnabled()` methods (e.g., next to `isThrottlingEnabled`, `isFullGCEnabled`):

**featureToggle=true:**
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

**featureToggle=false:**
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
