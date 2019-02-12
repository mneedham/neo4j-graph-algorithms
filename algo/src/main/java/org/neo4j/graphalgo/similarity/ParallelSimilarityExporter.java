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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.heavyweight.AdjacencyMatrix;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.*;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.DSSResult;
import org.neo4j.graphalgo.impl.GraphUnionFind;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.values.storable.Values;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ParallelSimilarityExporter extends StatementApi implements SimilarityExporter {

    private final Log log;
    private final int propertyId;
    private final int relationshipTypeId;
    private final int nodeCount;

    public ParallelSimilarityExporter(GraphDatabaseAPI api,
                                      Log log,
                                      String relationshipType,
                                      String propertyName, int nodeCount) {
        super(api);
        this.log = log;
        propertyId = getOrCreatePropertyId(propertyName);
        relationshipTypeId = getOrCreateRelationshipId(relationshipType);
        this.nodeCount = nodeCount;
    }

    public int export(Stream<SimilarityResult> similarityPairs, long batchSize) {
        IdMap idMap = new IdMap(this.nodeCount);
        AdjacencyMatrix adjacencyMatrix = new AdjacencyMatrix(this.nodeCount, false, AllocationTracker.EMPTY);
        WeightMap weightMap = new WeightMap(nodeCount, 0, propertyId);

        int[] numberOfRelationships = {0};

        similarityPairs.forEach(pair -> {
            int id1 = idMap.mapOrGet(pair.item1);
            int id2 = idMap.mapOrGet(pair.item2);
            adjacencyMatrix.addOutgoing(id1, id2);
            weightMap.put(RawValues.combineIntInt(id1, id2), pair.similarity);
            numberOfRelationships[0]++;
        });

        idMap.buildMappedIds();
        HeavyGraph graph = new HeavyGraph(idMap, adjacencyMatrix, weightMap, Collections.emptyMap());

        DisjointSetStruct dssResult = computePartitions(graph);

        Stream<List<DisjointSetStruct.InternalResult>> stream = dssResult.internalResultStream(graph)
                .collect(Collectors.groupingBy(item -> item.setId))
                .values()
                .stream();

        int queueSize = dssResult.getSetCount();

        if(queueSize == 0) {
            return 0;
        }

        log.info("ParallelSimilarityExporter: Relationships to be created: %d, Partitions found: %d", numberOfRelationships[0], queueSize);

        ArrayBlockingQueue<List<SimilarityResult>> outQueue = new ArrayBlockingQueue<>(queueSize);


        AtomicInteger inQueueBatchCount = new AtomicInteger(0);
        stream.parallel().forEach(partition -> {
            IntSet nodesInPartition = new IntHashSet();
            for (DisjointSetStruct.InternalResult internalResult : partition) {
                nodesInPartition.add(internalResult.internalNodeId);
            }

            List<SimilarityResult> inPartition = new ArrayList<>();
            List<SimilarityResult> outPartition = new ArrayList<>();

            for (DisjointSetStruct.InternalResult result : partition) {
                int nodeId = result.internalNodeId;
                graph.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, weight) -> {
                    SimilarityResult similarityRelationship = new SimilarityResult(idMap.toOriginalNodeId(sourceNodeId),
                            idMap.toOriginalNodeId(targetNodeId), -1, -1, -1, weight);

                    if (nodesInPartition.contains(targetNodeId)) {
                        inPartition.add(similarityRelationship);
                    } else {
                        outPartition.add(similarityRelationship);
                    }

                    return false;
                });
            }

            if (!inPartition.isEmpty()) {
                int inQueueBatches = writeSequential(inPartition.stream(), batchSize);
                inQueueBatchCount.addAndGet(inQueueBatches);
            }

            if (!outPartition.isEmpty()) {
                put(outQueue, outPartition);
            }
        });


        int inQueueBatches = inQueueBatchCount.get();


        int outQueueBatches = writeSequential(outQueue.stream().flatMap(Collection::stream), batchSize);
        log.info("ParallelSimilarityExporter: Batch Size: %d, Batches written - in parallel: %d, sequentially: %d", batchSize, inQueueBatches, outQueueBatches);
        return inQueueBatches + outQueueBatches;
    }

    private static <T> void put(BlockingQueue<T> queue, T items) {
        try {
            queue.put(items);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private DisjointSetStruct computePartitions(HeavyGraph graph) {
        GraphUnionFind algo = new GraphUnionFind(graph);
        DisjointSetStruct struct = algo.compute();
        algo.release();
        return struct;
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
                if (batch.size() > 0) {
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
