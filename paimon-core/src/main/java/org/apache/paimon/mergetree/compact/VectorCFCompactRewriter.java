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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;

/**
 * A {@link CompactRewriter} that merges low-valid-ratio vector CF files during full compaction.
 *
 * <p>During full compaction (outputLevel == maxLevel), this rewriter:
 *
 * <ol>
 *   <li>Pre-scans merged records to count live references per vector file
 *   <li>Identifies vector files with validRatio below threshold
 *   <li>Merges those vector files into a new file
 *   <li>Rewrites scalar files with updated VectorDescriptor references
 * </ol>
 *
 * <p>When vector compaction is not triggered (non-full compaction or all files above threshold), it
 * delegates to the standard {@link MergeTreeCompactRewriter} behavior.
 */
public class VectorCFCompactRewriter extends MergeTreeCompactRewriter {

    private static final Logger LOG = LoggerFactory.getLogger(VectorCFCompactRewriter.class);

    private final CoreOptions options;
    private final FileIO fileIO;
    private final RowType valueType;
    private final int maxLevel;
    private final List<DataFileMeta> bucketVectorFiles;

    public VectorCFCompactRewriter(
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            boolean snapshotSequenceOrdering,
            CoreOptions options,
            FileIO fileIO,
            RowType valueType,
            int maxLevel,
            List<DataFileMeta> bucketVectorFiles) {
        super(
                readerFactory,
                writerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mfFactory,
                mergeSorter,
                snapshotSequenceOrdering);
        this.options = options;
        this.fileIO = fileIO;
        this.valueType = valueType;
        this.maxLevel = maxLevel;
        this.bucketVectorFiles = bucketVectorFiles;
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        if (outputLevel != maxLevel || !options.vectorCFCompactEnabled()) {
            return super.rewrite(outputLevel, dropDelete, sections);
        }

        if (bucketVectorFiles.isEmpty()) {
            LOG.debug("No vector CF files in bucket, skipping vector compaction");
            return super.rewrite(outputLevel, dropDelete, sections);
        }

        LOG.info(
                "Full compaction with vector CF compact: {} vector files in bucket, "
                        + "threshold={}, minFiles={}",
                bucketVectorFiles.size(),
                options.vectorCFCompactValidRatioThreshold(),
                options.vectorCFCompactMinFiles());

        // TODO Phase 2: pre-scan to compute valid ratios
        // TODO Phase 3: merge low-ratio vector files
        // TODO Phase 4: rewrite with descriptor remapping

        // For now, delegate to normal compaction
        return rewriteCompaction(outputLevel, dropDelete, sections);
    }
}
