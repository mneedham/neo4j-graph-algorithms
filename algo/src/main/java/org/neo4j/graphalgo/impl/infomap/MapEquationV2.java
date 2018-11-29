package org.neo4j.graphalgo.impl.infomap;

import com.carrotsearch.hppc.ObjectArrayList;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.impl.Algorithm;

import java.util.Arrays;

import static org.neo4j.graphalgo.impl.infomap.MapEquationAlgorithm.entropy;

/**
 * @author mknblch
 */
public class MapEquationV2 extends Algorithm<MapEquationV2> implements MapEquationAlgorithm {

    private final Graph graph;
    private final NodeWeights pageRanks;
    private final RelationshipWeights weights;
    private final int nodeCount;
    private final ObjectArrayList<Module> modules;
    private final int[] communities;
    private final double totalGraphPageRankEntropy;

    public MapEquationV2(Graph graph, NodeWeights pageRanks, RelationshipWeights normalizedWeights) {
        this.graph = graph;
        this.pageRanks = pageRanks;
        this.weights = normalizedWeights;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.modules = new ObjectArrayList<>(nodeCount);
        this.communities = new int[nodeCount];
        Arrays.setAll(communities, i -> i);
        final double[] d = {0.};
        graph.forEachNode(node -> {
            d[0] += entropy(pageRanks.weightOf(node));
            modules.add(new Module(node));
            return true;
        });
        this.totalGraphPageRankEntropy = d[0];
    }

    public void compute() {


    }

    @Override
    public MapEquationV2 me() {
        return this;
    }

    @Override
    public MapEquationV2 release() {
        return this;
    }

    @Override
    public int[] getCommunities() {
        return new int[0];
    }

    @Override
    public double getMDL() {
        return 0;
    }

    @Override
    public double getIndexCodeLength() {
        return 0;
    }

    @Override
    public int getCommunityCount() {
        return 0;
    }

    @Override
    public double getModuleCodeLength() {
        return 0;
    }

    @Override
    public int getIterations() {
        return 0;
    }

    private class Module {

        // number of nodes
        int n = 1;
        // ergodic frequency
        double p;
        // exit probability without teleport
        double w = 0;
        // exit probability with teleport
        double q;

        public Module(int startNode) {
            p = pageRanks.weightOf(startNode);
        }
    }
}
