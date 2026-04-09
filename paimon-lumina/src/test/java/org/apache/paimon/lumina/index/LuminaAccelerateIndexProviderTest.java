/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.lumina.index;

import org.apache.paimon.accelerateindex.AccelerateIndexBuilder;
import org.apache.paimon.accelerateindex.AccelerateIndexProvider;
import org.apache.paimon.accelerateindex.AccelerateIndexScanner;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LuminaAccelerateIndexProvider}. */
class LuminaAccelerateIndexProviderTest {

    @Test
    void testIdentifier() {
        LuminaAccelerateIndexProvider provider = new LuminaAccelerateIndexProvider();
        assertThat(provider.identifier()).isEqualTo("lumina");
    }

    @Test
    void testCreateBuilderReturnsNonNull() {
        AccelerateIndexProvider provider = new LuminaAccelerateIndexProvider();
        AccelerateIndexBuilder builder = provider.createBuilder();
        assertThat(builder).isNotNull();
        assertThat(builder).isInstanceOf(LuminaAccelerateIndexBuilder.class);
    }

    @Test
    void testCreateScannerReturnsNonNull() {
        AccelerateIndexProvider provider = new LuminaAccelerateIndexProvider();
        AccelerateIndexScanner scanner = provider.createScanner();
        assertThat(scanner).isNotNull();
        assertThat(scanner).isInstanceOf(LuminaAccelerateIndexScanner.class);
    }
}
