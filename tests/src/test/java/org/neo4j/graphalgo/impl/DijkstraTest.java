package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.LongArrayList;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.impl.yens.Dijkstra;
import org.neo4j.graphalgo.impl.yens.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.*;

/**
 * Test for specialized dijkstra implementation with filters & maxDepth
 *
 *
 * Graph:
 *
 *     (b)   (e)
 *   2/ 1\ 2/ 1\
 * >(a)  (d)  ((g))
 *   1\ 2/ 1\ 2/
 *    (c)   (f)
 *
 * @author mknblch
 */
public class DijkstraTest {

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static Graph graph;
    private static LongArrayList edgeBlackList;
    private static Dijkstra dijkstra;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE {cost:2.0}]->(b),\n" +
                        " (a)-[:TYPE {cost:1.0}]->(c),\n" +
                        " (b)-[:TYPE {cost:1.0}]->(d),\n" +
                        " (c)-[:TYPE {cost:2.0}]->(d),\n" +
                        " (d)-[:TYPE {cost:1.0}]->(e),\n" +
                        " (d)-[:TYPE {cost:2.0}]->(f),\n" +
                        " (e)-[:TYPE {cost:2.0}]->(g),\n" +
                        " (f)-[:TYPE {cost:1.0}]->(g)";

        db.execute(cypher);

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .asUndirected(true)
                .load(HeavyGraphFactory.class);

        edgeBlackList = new LongArrayList();

        dijkstra = new Dijkstra(graph)
                .withDirection(Direction.OUTGOING)
                .withFilter((s, t, r) -> !edgeBlackList.contains(RawValues.combineIntInt(s, t)));
    }

    private static int id(String name) {
        final Node[] node = new Node[1];
        db.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return graph.toMappedNodeId(node[0].getId());
    }

    @Test
    public void testNoFilter() throws Exception {
        edgeBlackList.clear();
        final WeightedPath weightedPath = dijkstra();
        assertEquals(5, weightedPath.size());
        System.out.println("weightedPath = " + weightedPath);
    }

    @Test
    public void testFilterACDF() throws Exception {
        edgeBlackList.clear();
        edgeBlackList.add(RawValues.combineIntInt(id("a"), id("c")));
        edgeBlackList.add(RawValues.combineIntInt(id("d"), id("f")));
        final WeightedPath weightedPath = dijkstra();
        assertEquals(5, weightedPath.size());
        assertTrue(weightedPath.containsNode(id("b")));
        assertTrue(weightedPath.containsNode(id("e")));
    }

    @Test
    public void testFilterABDE() throws Exception {
        edgeBlackList.clear();
        edgeBlackList.add(RawValues.combineIntInt(id("a"), id("b")));
        edgeBlackList.add(RawValues.combineIntInt(id("d"), id("e")));
        final WeightedPath weightedPath = dijkstra();
        assertEquals(5, weightedPath.size());
        assertTrue(weightedPath.containsNode(id("c")));
        assertTrue(weightedPath.containsNode(id("f")));
    }

    @Test
    public void testMaxDepth() throws Exception {
        assertTrue(dijkstra.compute(id("a"), id("d"), 4).isPresent());
        assertFalse(dijkstra.compute(id("a"), id("d"), 3).isPresent());
    }

    private WeightedPath dijkstra() {
        return dijkstra.compute(id("a"), id("g"))
                .orElseThrow(() -> new AssertionError("No path"));
    }

}