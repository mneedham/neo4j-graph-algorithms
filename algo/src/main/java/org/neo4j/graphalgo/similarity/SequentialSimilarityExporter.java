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
import org.neo4j.logging.Log;
import org.neo4j.values.storable.Values;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class SequentialSimilarityExporter extends StatementApi implements SimilarityExporter {

    private final Log log;
    private final int propertyId;
    private final int relationshipTypeId;

    public SequentialSimilarityExporter(GraphDatabaseAPI api,
                                        Log log, String relationshipType,
                                        String propertyName) {
        super(api);
        this.log = log;
        propertyId = getOrCreatePropertyId(propertyName);
        relationshipTypeId = getOrCreateRelationshipId(relationshipType);
    }

    public int export(Stream<SimilarityResult> similarityPairs, long batchSize) {
        int batches = writeSequential(similarityPairs, batchSize);
        log.info("SequentialSimilarityExporter: Batch Size: %d, Batches written - sequentially: %d", batchSize, batches);
        return batches;
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

    private int writeSequential(Stream<SimilarityResult> similarityPairs, long batchSize) {
        log.info("SequentialSimilarityExporter: Writing relationships...");
        int[] counter = {0};
        if (batchSize == 1) {
            similarityPairs.forEach(similarityResult -> {
                export(similarityResult);
                counter[0]++;
            });
        } else {
            Iterator<SimilarityResult> iterator = similarityPairs.iterator();
            do {
                List<SimilarityResult> batch = take(iterator, Math.toIntExact(batchSize));
                export(batch);
                if(batch.size() > 0) {
                    counter[0]++;
                }
            } while (iterator.hasNext());
        }

        return counter[0];
    }


    private static List<SimilarityResult> take(Iterator<SimilarityResult> iterator, int batchSize) {
        List<SimilarityResult> result = new ArrayList<>(batchSize);
        while (iterator.hasNext() && batchSize-- > 0) {
            result.add(iterator.next());
        }
        return result;
    }


}
