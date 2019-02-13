package org.neo4j.graphalgo.algo.linkprediction;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphalgo.linkprediction.NeighborsFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;

public class NeighborsFinderTest {

    @Rule
    public final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private GraphDatabaseAPI api;
    public static final RelationshipType FRIEND = RelationshipType.withName("FRIEND");
    public static final RelationshipType COLLEAGUE = RelationshipType.withName("COLLEAGUE");
    public static final RelationshipType FOLLOWS = RelationshipType.withName("FOLLOWS");

    @Before
    public void setup() {
        api = DB.getGraphDatabaseAPI();
    }

    @Test
    public void excludeDirectRelationships() throws Throwable {
        try (Transaction tx = api.beginTx()) {
            Node node1 = api.createNode();
            Node node2 = api.createNode();
            node1.createRelationshipTo(node2, FRIEND);
            tx.success();
        }

        NeighborsFinder neighborsFinder = new NeighborsFinder(api);

        try (Transaction tx = api.beginTx()) {
            Node node1 = api.getNodeById(0);
            Node node2 = api.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, null, Direction.BOTH);

            assertEquals(0, neighbors.size());
        }
    }

    @Test
    public void sameNodeHasNoCommonNeighbors() throws Throwable {
        try (Transaction tx = api.beginTx()) {
            Node node1 = api.createNode();
            Node node2 = api.createNode();
            node1.createRelationshipTo(node2, FRIEND);
            tx.success();
        }

        NeighborsFinder neighborsFinder = new NeighborsFinder(api);

        try (Transaction tx = api.beginTx()) {
            Node node1 = api.getNodeById(0);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node1, null, Direction.BOTH);

            assertEquals(0, neighbors.size());
        }
    }

    @Test
    public void findNeighborsExcludingDirection() throws Throwable {

        try (Transaction tx = api.beginTx()) {
            Node node1 = api.createNode();
            Node node2 = api.createNode();
            Node node3 = api.createNode();
            Node node4 = api.createNode();

            node1.createRelationshipTo(node3, FRIEND);
            node2.createRelationshipTo(node3, FRIEND);
            node1.createRelationshipTo(node4, COLLEAGUE);
            node2.createRelationshipTo(node4, COLLEAGUE);

            tx.success();
        }

        NeighborsFinder neighborsFinder = new NeighborsFinder(api);

        try (Transaction tx = api.beginTx()) {
            Node node1 = api.getNodeById(0);
            Node node2 = api.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, null, Direction.BOTH);

            assertEquals(2, neighbors.size());
        }
    }

    @Test
    public void findOutgoingNeighbors() throws Throwable {

        try (Transaction tx = api.beginTx()) {
            Node node1 = api.createNode();
            Node node2 = api.createNode();
            Node node3 = api.createNode();

            node1.createRelationshipTo(node3, FOLLOWS);
            node2.createRelationshipTo(node3, FOLLOWS);

            tx.success();
        }

        NeighborsFinder neighborsFinder = new NeighborsFinder(api);

        try (Transaction tx = api.beginTx()) {
            Node node1 = api.getNodeById(0);
            Node node2 = api.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, FOLLOWS, Direction.OUTGOING);

            assertEquals(1, neighbors.size());
        }
    }

    @Test
    public void findIncomingNeighbors() throws Throwable {

        try (Transaction tx = api.beginTx()) {
            Node node1 = api.createNode();
            Node node2 = api.createNode();
            Node node3 = api.createNode();

            node3.createRelationshipTo(node1, FOLLOWS);
            node3.createRelationshipTo(node2, FOLLOWS);

            tx.success();
        }

        NeighborsFinder neighborsFinder = new NeighborsFinder(api);

        try (Transaction tx = api.beginTx()) {
            Node node1 = api.getNodeById(0);
            Node node2 = api.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, FOLLOWS, Direction.INCOMING);

            assertEquals(1, neighbors.size());
        }
    }

    @Test
    public void findNeighborsOfSpecificRelationshipType() throws Throwable {

        try (Transaction tx = api.beginTx()) {
            Node node1 = api.createNode();
            Node node2 = api.createNode();
            Node node3 = api.createNode();
            Node node4 = api.createNode();

            node1.createRelationshipTo(node3, FRIEND);
            node2.createRelationshipTo(node3, FRIEND);
            node1.createRelationshipTo(node4, COLLEAGUE);
            node2.createRelationshipTo(node4, COLLEAGUE);

            tx.success();
        }

        NeighborsFinder neighborsFinder = new NeighborsFinder(api);

        try (Transaction tx = api.beginTx()) {
            Node node1 = api.getNodeById(0);
            Node node2 = api.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, COLLEAGUE, Direction.BOTH);

            assertEquals(1, neighbors.size());
        }
    }

    @Test
    public void dontCountDuplicates() throws Throwable {

        Node node1;
        Node node2;
        Node node3;
        Node node4;
        try (Transaction tx = api.beginTx()) {
            node1 = api.createNode();
            node2 = api.createNode();
            node3 = api.createNode();
            node4 = api.createNode();

            node1.createRelationshipTo(node3, FRIEND);
            node2.createRelationshipTo(node3, FRIEND);
            node1.createRelationshipTo(node4, COLLEAGUE);
            node2.createRelationshipTo(node4, COLLEAGUE);

            tx.success();
        }

        NeighborsFinder neighborsFinder = new NeighborsFinder(api);

        try (Transaction tx = api.beginTx()) {
            Set<Node> neighbors = neighborsFinder.findNeighbors(node1, node2, null, Direction.BOTH);

            assertEquals(2, neighbors.size());
            assertThat(neighbors, hasItems(node3, node4));
        }
    }

    @Test
    public void otherNodeCountsAsNeighbor() throws Throwable {

        Node node1;
        Node node2;
        try (Transaction tx = api.beginTx()) {
            node1 = api.createNode();
            node2 = api.createNode();
            node1.createRelationshipTo(node2, FRIEND);

            tx.success();
        }

        NeighborsFinder neighborsFinder = new NeighborsFinder(api);

        try (Transaction tx = api.beginTx()) {
            Set<Node> neighbors = neighborsFinder.findNeighbors(node1, node2, null, Direction.BOTH);

            assertEquals(2, neighbors.size());
            assertThat(neighbors, hasItems(node1, node2));
        }
    }

    @Test
    public void otherNodeCountsAsOutgoingNeighbor() throws Throwable {
        Node node1;
        Node node2;
        try (Transaction tx = api.beginTx()) {
            node1 = api.createNode();
            node2 = api.createNode();
            node1.createRelationshipTo(node2, FRIEND);

            tx.success();
        }

        NeighborsFinder neighborsFinder = new NeighborsFinder(api);

        try (Transaction tx = api.beginTx()) {
            Set<Node> neighbors = neighborsFinder.findNeighbors(node1, node2, null, Direction.OUTGOING);

            assertEquals(1, neighbors.size());
            assertThat(neighbors, hasItems(node2));
        }
    }

}

