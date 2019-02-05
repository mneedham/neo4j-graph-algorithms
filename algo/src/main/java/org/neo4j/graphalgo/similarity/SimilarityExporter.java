/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.StatementApi;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SimilarityExporter extends StatementApi {

    private final int propertyId;
    private final int relationshipTypeId;

    public SimilarityExporter(GraphDatabaseAPI api,
                              String relationshipType,
                              String propertyName) {
        super(api);
        propertyId = getOrCreatePropertyId(propertyName);
        relationshipTypeId = getOrCreateRelationshipId(relationshipType);
    }

    public void export(Stream<SimilarityResult> similarityPairs, long batchSize) {
        writeSequential(similarityPairs, batchSize);
    }

    private void export(SimilarityResult similarityResult) {
        applyInTransaction(statement -> {
            try {
                createRelationship(similarityResult, statement);
            } catch (KernelException e) {
                ExceptionUtil.throwKernelException(e);
            }
            return null;
        });

    }

    private void export(List<SimilarityResult> similarityResults) {
        applyInTransaction(statement -> {
            for (SimilarityResult similarityResult : similarityResults) {
                try {
                    createRelationship(similarityResult, statement);
                } catch (KernelException e) {
                    ExceptionUtil.throwKernelException(e);
                }
            }
            return null;
        });

    }

    private void createRelationship(SimilarityResult similarityResult, KernelTransaction statement) throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException {
        long node1 = similarityResult.item1;
        long node2 = similarityResult.item2;
        long relationshipId = statement.dataWrite().relationshipCreate(node1, relationshipTypeId, node2);

        statement.dataWrite().relationshipSetProperty(
                relationshipId, propertyId, Values.doubleValue(similarityResult.similarity));
    }

    private int getOrCreateRelationshipId(String relationshipType) {
        return applyInTransaction(stmt -> stmt
                .tokenWrite()
                .relationshipTypeGetOrCreateForName(relationshipType));
    }

    private int getOrCreatePropertyId(String propertyName) {
        return applyInTransaction(stmt -> stmt
                .tokenWrite()
                .propertyKeyGetOrCreateForName(propertyName));
    }

    private void writeSequential(Stream<SimilarityResult> similarityPairs, long batchSize) {
        if (batchSize == 1) {
            similarityPairs.forEach(this::export);
        } else {
            batchStream(similarityPairs, batchSize).forEach(this::export);
        }
    }

    public static Stream<List<SimilarityResult>> batchStream(Stream<SimilarityResult> stream, long batchSize) {
        return batchSize <= 0
                ? Stream.of(stream.collect(Collectors.toList()))
                : StreamSupport.stream(new BatchSpliterator(stream.spliterator(), batchSize), stream.isParallel());
    }

    private static class BatchSpliterator implements Spliterator<List<SimilarityResult>> {
        private final Spliterator<SimilarityResult> stream;
        private final long batchSize;

        public BatchSpliterator(Spliterator<SimilarityResult> stream, long batchSize) {
            this.stream = stream;
            this.batchSize = batchSize;
        }

        @Override
        public boolean tryAdvance(Consumer<? super List<SimilarityResult>> action) {
            final List<SimilarityResult> batch = new ArrayList<>(Math.toIntExact(batchSize));
            for (int i = 0; i < batchSize && stream.tryAdvance(batch::add); i++)
                ;
            if (batch.isEmpty())
                return false;
            action.accept(batch);
            return true;
        }

        @Override
        public Spliterator<List<SimilarityResult>> trySplit() {
            if (stream.estimateSize() <= batchSize)
                return null;
            final Spliterator<SimilarityResult> splitStream = this.stream.trySplit();
            return splitStream == null ? null : new BatchSpliterator(splitStream, batchSize);
        }

        @Override
        public long estimateSize() {
            final double estimatedSize = stream.estimateSize();
            return estimatedSize == 0 ? 0 : (long) Math.ceil(estimatedSize / (double) batchSize);
        }

        @Override
        public int characteristics() {
            return stream.characteristics();
        }

    }


}
