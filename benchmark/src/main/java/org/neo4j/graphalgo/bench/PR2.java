/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.bench;

import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

public final class PR2 extends BaseMain {

    private boolean runHuge = true;
    private boolean runHeavy = true;

    @Override
    void init(final Collection<String> args) {
        runHuge = !args.contains("heavy");
        runHeavy = !args.contains("huge");
    }

    @Override
    Iterable<String> run(String graphToLoad, final Log log) throws Throwable {
        GraphDatabaseAPI db = LdbcDownloader.openDb(graphToLoad);

        AllocationTracker trackerHuge = AllocationTracker.create();
        AllocationTracker trackerHeavy = AllocationTracker.create();

        GraphLoader graphLoader = new GraphLoader(db)
//                .withoutExecutorService()
                .withExecutorService(Pools.DEFAULT)
//                .withConcurrency(1)
                .withLog(log)
                .withLogInterval(500, TimeUnit.MILLISECONDS)
//                    .withLabel("Person")
                .withAllocationTracker(trackerHuge)
                .withDirection(Direction.OUTGOING)
                .withSort(true)
                .asUndirected(true)
                .withoutRelationshipWeights();
//                .withOptionalRelationshipWeightsFromProperty(
//                        "creationDate", 1.0
//                );


        System.gc();

        Graph graph = null;
        Graph heavy = null;
        List<String> messages = new ArrayList<>();

        try {

            if (runHuge) {
                jprofBookmark("start huge load");

                try (ProgressTimer ignored = ProgressTimer.start(time -> messages.add(String.format(
                        "huge load: %d ms",
                        time)))) {
                    graph = graphLoader.load(HugeGraphFactory.class);
                }

                jprofBookmark("end huge load");

                System.out.println("after loading huge");
                System.gc();

                jprofBookmark("huge usage");
//                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10L));
            }

            if (runHeavy) {
                jprofBookmark("start heavy load");

                try (ProgressTimer ignored = ProgressTimer.start(time -> messages.add(String.format(
                        "heavy load: %d ms",
                        time)))) {
                    heavy = graphLoader.withAllocationTracker(trackerHeavy).load(HeavyGraphFactory.class);
                }

                jprofBookmark("end heavy load");

                System.out.println("after loading heavy");
                System.gc();

                jprofBookmark("heavy usage");
//                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10L));
            }
        } catch (Exception e) {
            for (final Throwable throwable : flattenSuppressed(e)) {
                throwable.printStackTrace();
            }
            throw e;
        } finally {
            if (graph != null && heavy != null) {
                final Graph heavyGraph = heavy;
                HugeGraph hugeGraph = (HugeGraph) graph;
                try (ProgressTimer ignored = ProgressTimer.start(time -> messages.add(String.format(
                        "traversal: %d ms",
                        time)))) {
                    LongArrayList mismatches = new LongArrayList();
                    long[] matches = new long[1];
                    hugeGraph.forEachNode((long node) -> {
                        int degree = heavyGraph.degree((int) node, Direction.OUTGOING);
                        long[] calcDegree = new long[1];
                        try {
                            hugeGraph.forEachOutgoing(node, (src, tgt) -> {
                                ++calcDegree[0];
                                return true;
                            });
                        } catch (RuntimeException e) {
                            throw new RuntimeException(String.format(
                                    "Error in (%d)--> after %d relationships",
                                    node,
                                    calcDegree[0]), e);
                        }
                        if (degree == calcDegree[0]) {
                            ++matches[0];
                        } else {
                            mismatches.add(node);
                        }
                        return true;
                    });

                    messages.add("matches = " + matches[0]);
                    messages.add("mismatches = " + mismatches.size());
                    if (!mismatches.isEmpty()) {
                        long[] first10Mismatches = StreamSupport.stream(mismatches.spliterator(), false)
                                .mapToLong(cursor -> cursor.value)
                                .limit(10)
                                .toArray();
                        messages.add("first10Mismatches = " + Arrays.toString(first10Mismatches));
                    }
                } finally {
                    System.out.println("after traversing");
                    System.gc();
                }
            }
//            jprofBookmark("shutdown");

            if (graph != null) {
                graph.release();
                graph = null;
            }
            if (heavy != null) {
                heavy.release();
                heavy = null;
            }
            System.out.println("after graph.release");
            System.gc();

            db.shutdown();
            db = null;
            Pools.DEFAULT.shutdownNow();
            System.out.println("after DB shutdown");
            System.gc();
        }
        return messages;
    }

    public static void main(final String... args) throws Throwable {
        BaseMain.run(PR2.class, args);
    }
}
