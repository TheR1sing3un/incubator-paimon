<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Paimon Query Service SDK

This module provides `org.apache.paimon.query.sdk.PaimonRemoteLookupClient` for Java point lookup
through Paimon Query Service.

## Downstream Dependency

```xml
<dependency>
    <groupId>org.apache.paimon</groupId>
    <artifactId>paimon-query-service-sdk</artifactId>
    <version>${paimon.version}</version>
</dependency>
```

The SDK intentionally hides heavy internals from downstream:

1. `paimon-bundle` and `paimon-service-runtime` are excluded transitively.
2. You should not add them back unless you have a very specific reason.
3. Keep only this SDK dependency in normal cases.
4. The SDK includes Log4j 1.x compatibility API (`log4j-1.2-api`) for Hadoop/Hive runtime.

## Logging Compatibility

If your app only depends on this SDK, it should not fail with
`NoClassDefFoundError: org/apache/log4j/Level`.

If you need a full custom Log4j2 configuration in your app, add your preferred logging
implementation explicitly (for example `log4j-core` and `log4j-slf4j-impl`) at application level.

## Notes For Fat-Jar Packaging

If your application builds an uber/fat jar, use `maven-shade-plugin` and merge Log4j2 plugin
metadata. Otherwise you may see runtime logging errors.

Recommended:

1. Use `maven-shade-plugin` (not `jar-with-dependencies`).
2. Add Log4j2 cache transformer.
3. Keep only one logging implementation in the final jar.

Example:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>3.4.1</version>
    <dependencies>
        <dependency>
            <groupId>com.github.edwgiz</groupId>
            <artifactId>maven-shade-plugin.log4j2-cachefile-transformer</artifactId>
            <version>2.8.1</version>
        </dependency>
    </dependencies>
    <executions>
        <execution>
            <phase>package</phase>
            <goals>
                <goal>shade</goal>
            </goals>
            <configuration>
                <transformers>
                    <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                    <transformer implementation="com.github.edwgiz.mavenShadePlugin.log4j2CacheTransformer.PluginsCacheFileTransformer"/>
                </transformers>
            </configuration>
        </execution>
    </executions>
</plugin>
```
