package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.StatementApi;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public class LouvainCommunityExporter extends StatementApi {

    private final ExecutorService pool;
    private final int concurrency;
    private final IdMapping mapping;
    private final int nodeCount;
    private int offset = 0;
    private Integer propertyId;

    public LouvainCommunityExporter(GraphDatabaseAPI api,
                                    ExecutorService pool,
                                    int concurrency,
                                    IdMapping mapping,
                                    int nodeCount,
                                    String propertyName) {
        super(api);
        this.pool = pool;
        this.concurrency = concurrency;
        this.mapping = mapping;
        this.nodeCount = nodeCount;

        try {
            propertyId = applyInTransaction(statement -> statement.tokenWriteOperations().propertyKeyGetOrCreateForName(propertyName));
        } catch (IllegalTokenNameException e) {
            throw new RuntimeException(e);
        }
    }

    public void export(int[][] communities) {
        final Collection<PrimitiveIntIterable> batchIterables = ParallelUtil.batchIterables(concurrency, nodeCount);
        final ArrayList<Runnable> tasks = new ArrayList<>();
        batchIterables.forEach(it -> tasks.add(new NodeBatchExporter(it, communities)));
        ParallelUtil.run(tasks, pool);
    }

    private class NodeBatchExporter implements Runnable {

        private final PrimitiveIntIterable iterable;
        private final int[][] communities;

        private NodeBatchExporter(PrimitiveIntIterable iterable, int[][] communities) {
            this.iterable = iterable;
            this.communities = communities;
        }

        @Override
        public void run() {
            try {
                acceptInTransaction(statement -> {
                    final DataWriteOperations dataWriteOperations = statement.dataWriteOperations();
                    for(PrimitiveIntIterator it = iterable.iterator(); it.hasNext(); ) {
                        final int id = it.next();
                        // build int array
                        final int[] data = new int[offset];
                        for (int i = 0; i < offset; i++) {
                            data[i] = communities[i][id];
                        }
                        dataWriteOperations.nodeSetProperty(
                                mapping.toOriginalNodeId(id),
                                DefinedProperty.intArrayProperty(propertyId, data));
                    }
                });
            } catch (KernelException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
