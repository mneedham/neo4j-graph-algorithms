package org.neo4j.graphalgo.core.heavyweight;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.cursors.LongIntCursor;
import com.carrotsearch.hppc.procedures.LongIntProcedure;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory.*;

class NodeLoader {
    private final GraphSetup setup;
    private final GraphDimensions dimensions;
    private final GraphDatabaseAPI api;

    public NodeLoader(GraphDatabaseAPI api, GraphSetup setup, GraphDimensions dimensions) {
        this.api = api;
        this.setup = setup;
        this.dimensions = dimensions;
    }

    public Nodes load() {
        int batchSize = setup.batchSize;
        return CypherLoadingUtils.canBatchLoad(setup.loadConcurrent(), batchSize, setup.startLabel) ?
                batchLoadNodes(batchSize) :
                loadNodes(0, NO_BATCH);
    }

    private Nodes batchLoadNodes(int batchSize) {
        ExecutorService pool = setup.executor;
        int threads = setup.concurrency();

        // data structures for merged information
        int capacity = INITIAL_NODE_COUNT * 10;
        LongIntHashMap nodeToGraphIds = new LongIntHashMap(capacity);

        Map<PropertyMapping, WeightMap> nodeProperties = nodeProperties(capacity);

        long offset = 0;
        long total = 0;
        long lastOffset = 0;
        List<Future<Nodes>> futures = new ArrayList<>(threads);
        boolean working = true;
        do {
            long skip = offset;
            futures.add(pool.submit(() -> loadNodes(skip, batchSize)));
            offset += batchSize;
            if (futures.size() >= threads) {
                for (Future<Nodes> future : futures) {
                    Nodes result = get("Error during loading nodes offset: " + (lastOffset + batchSize), future);
                    lastOffset = result.offset();
                    total += result.rows();
                    working = result.idMap.size() > 0;
                    if (working) {
                        int minNodeId = nodeToGraphIds.size();

                        result.idMap.nodeToGraphIds().forEach(
                                (LongIntProcedure) (graphId, algoId) -> {
                                    int newId = algoId + minNodeId;
                                    nodeToGraphIds.put(graphId, newId);

                                    for (Map.Entry<PropertyMapping, WeightMap> entry : nodeProperties.entrySet()) {
                                        entry.getValue().put(
                                                newId,
                                                result.nodeProperties().get(entry.getKey()).get(algoId));
                                    }
                                });
                    }
                }
                futures.clear();
            }
        } while (working);

        long[] graphIds = new long[nodeToGraphIds.size()];
        for (final LongIntCursor cursor : nodeToGraphIds) {
            graphIds[cursor.value] = cursor.key;
        }
        return new Nodes(
                0L,
                total,
                new IdMap(graphIds,nodeToGraphIds),
                null,null,
                nodeProperties,
                setup.nodeDefaultWeight,
                setup.nodeDefaultPropertyValue
        );
    }

    private Nodes loadNodes(long offset, int batchSize) {
        int capacity = batchSize == NO_BATCH ? INITIAL_NODE_COUNT : batchSize;
        final IdMap idMap = new IdMap(capacity);

        Map<PropertyMapping, WeightMap> nodeProperties = nodeProperties(capacity);


        NodeRowVisitor visitor = new NodeRowVisitor(idMap, nodeProperties);
        api.execute(setup.startLabel, params(offset, batchSize)).accept(visitor);
        idMap.buildMappedIds();
        return new Nodes(
                offset,
                visitor.rows(),
                idMap,
                null,
                null,
                nodeProperties,
                setup.nodeDefaultWeight,
                setup.nodeDefaultPropertyValue);
    }

    private Map<PropertyMapping, WeightMap> nodeProperties(int capacity) {
        Map<PropertyMapping, WeightMap> nodeProperties = new HashMap<>();
        for (PropertyMapping propertyMapping : setup.nodePropertyMappings) {
            nodeProperties.put(propertyMapping,
                    CypherLoadingUtils.newWeightMapping(true, dimensions.nodePropertyDefaultValue(propertyMapping.propertyName), capacity));
        }
        return nodeProperties;
    }

    private <T> T get(String message, Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted: " + message, e);
        } catch (ExecutionException e) {
            throw new RuntimeException(message, e);
        }
    }

    private Map<String, Object> params(long offset, int batchSize) {
        Map<String, Object> params = new HashMap<>(setup.params);
        params.put(SKIP, offset);
        if (batchSize > 0) {
            params.put(LIMIT, batchSize);
        }
        return params;
    }

}
