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
package org.neo4j.graphalgo.algo;

import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
//import org.neo4j.graphalgo.impl.infomap.InfoMap;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * (a)-(b)-(d)
 * | /            -> (abc)-(d)
 * (c)
 *
 * @author mknblch
 */
@RunWith(Parameterized.class)
@Ignore
public class LouvainPerformanceTest {

    private static final String nodes =
            "    WITH [\"Jennifer\",\"Michelle\",\"Tanya\",\"Julie\",\"Christie\",\"Sophie\",\"Amanda\",\"Khloe\",\"Sarah\",\"Kaylee\"] AS names \n" +
                    "    FOREACH (r IN range(0,1000) | CREATE (:User {username:names[r % size(names)]+r}));";

    private static final String rels =
            "MATCH (u1:User),(u2:User)\n" +
                    "    WITH u1,u2\n" +
                    "    LIMIT 50000\n" +
                    "    WHERE rand() < 0.1\n" +
                    "    CREATE (u1)-[:FRIENDS {weight: rand()}]->(u2);";

    public static void main(String[] args) {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("/tmp/louvain-perf"));
//        loadData(db);
//        runLouvainAlgorithm((GraphDatabaseAPI) db);
//        runInfoMap((GraphDatabaseAPI) db);
        db.shutdown();
    }

    private static void loadData(GraphDatabaseService db) {
        db.execute(nodes).close();
        db.execute(rels).close();
    }

    private static void runLouvainAlgorithm(GraphDatabaseAPI db) {
        Graph graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .asUndirected(true)
                .withOptionalRelationshipWeightsFromProperty("weight2", 1.0)
                .load(HeavyGraphFactory.class);

        long start = System.nanoTime();
        new Louvain(graph, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .compute(10, 10);

        System.out.println("Time: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    }

//    private static void runInfoMap(GraphDatabaseAPI db) {
//        Graph graph = new GraphLoader(db)
//                .withAnyRelationshipType()
//                .withAnyLabel()
//                .withoutNodeProperties()
//                .asUndirected(true)
//                .withOptionalRelationshipWeightsFromProperty("weight2", 1.0)
//                .load(HeavyGraphFactory.class);
//
//        long start = System.nanoTime();
//        InfoMap infoMap = InfoMap.unweighted(graph, 10, 0.005, 0.15, Pools.FJ_POOL, 8, TestProgressLogger.INSTANCE, TerminationFlag.RUNNING_TRUE);
//        infoMap.compute();
//
//        System.out.println("communities:" + infoMap.getCommunityCount());
//        System.out.println("communities:" + Arrays.toString(infoMap.getCommunities()));
//
//        System.out.println("Time: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
//    }

    //        AtomicInteger counter = new AtomicInteger(0);
//        for (int i = 0; i < graph.nodeCount(); i++) {
//            graph.forEachOutgoing(i, new RelationshipConsumer() {
//                @Override
//                public boolean accept(int sourceNodeId, int targetNodeId, long relationId) {
//                    counter.incrementAndGet();
//                    return true;
//                }
//
//            });
//        }
}
