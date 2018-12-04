package org.neo4j.graphalgo.impl.infomap;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.impl.Algorithm;

import java.util.Arrays;

import static org.neo4j.graphalgo.impl.infomap.MapEquation.TAU;
import static org.neo4j.graphalgo.impl.infomap.MapEquationAlgorithm.entropy;

/**
 * @author mknblch
 */
public class MapEquationV2 extends Algorithm<MapEquationV2> implements MapEquationAlgorithm {

    private final Graph graph;
    private final NodeWeights pageRanks;
    private final RelationshipWeights weights;
    private final int nodeCount;
    private final IntObjectMap<Module> modules;
    private final int[] communities;
    private final double totalGraphPageRankEntropy;

    public MapEquationV2(Graph graph, NodeWeights pageRanks, RelationshipWeights normalizedWeights) {
        this.graph = graph;
        this.pageRanks = pageRanks;
        this.weights = normalizedWeights;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.modules = new IntObjectScatterMap<>(nodeCount);
        this.communities = new int[nodeCount];
        Arrays.setAll(communities, i -> i);
        final double[] d = {0.};
        graph.forEachNode(node -> {
            d[0] += entropy(pageRanks.weightOf(node));
            modules.put(node, new Module(node));
            return true;
        });
        this.totalGraphPageRankEntropy = d[0];
    }

    public void merge(int modA, int modB) {
        modules.get(modA).merge(modules.remove(modB));


//        System.out.println("\tmodules = " + modules);

    }

    @Override
    public double getMDL() {
        double sumQi = 0;
        double sumplogpQi = 0;
        double sumPQi = 0;

        for (IntObjectCursor<Module> cursor : modules) {
            final Module module = cursor.value;
            sumQi += module.q;
            sumplogpQi += entropy(module.q);
            sumPQi += entropy(module.p + module.q);
        }

        return entropy(sumQi) - 2 * sumplogpQi - totalGraphPageRankEntropy + sumPQi;
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
        return communities;
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


    double weightSum(Module l) {
        final Pointer.DoublePointer w = Pointer.wrap(.0);
        l.nodes.forEach((IntProcedure) n -> {
            graph.forEachOutgoing(n, (s, t, r) -> {
                if (communities[t] != l.moduleIndex) {
                    w.v += weights.weightOf(s, t);
                }
                return true;
            });
        });
        return w.v;
    }

    double interModuleWeight(Module j, Module k) {
        final Pointer.DoublePointer w = Pointer.wrap(.0);
        j.nodes.forEach((IntProcedure) c -> {
            graph.forEachOutgoing(c, (s, t, r) -> {
                if (communities[t] == k.moduleIndex) {
                    w.v += weights.weightOf(s, t);
                }
                return true;
            });
        });
        return w.v;
    }

    double Wj_k(Module j, Module k) {
        return interModuleWeight(j, k) + interModuleWeight(k, j);
    }

    private class Module {

        private final int moduleIndex;
        // nodes
        IntSet nodes;
        // ergodic frequency
        double p;
        // exit probability without teleport
        double w;
        // exit probability with teleport
        double q;

        public Module(int startNode) {

            this.moduleIndex = startNode;

            nodes = new IntScatterSet();
            nodes.add(startNode);

            p = pageRanks.weightOf(startNode);
            final Pointer.DoublePointer initW = Pointer.wrap(.0);
            // sum of all weights
            graph.forEachOutgoing(startNode, (s, t, r) -> {
                if (s == t) {
                    return true;
                }
                initW.v += weights.weightOf(s, t);
                return true;
            });
            w = p * initW.v;
            q = TAU * p + (1. - TAU) * w;
        }

        public void merge(Module other) {


            System.out.printf("\tmerge %d <= { %d (+) %d }%n", moduleIndex, moduleIndex, other.moduleIndex);

            p += other.p;
            w = w + other.w - Wj_k(this, other);

            other.nodes.forEach((IntProcedure) nodes::add);
            nodes.forEach((IntProcedure) node -> communities[node] = moduleIndex);

            q = TAU * (((double) nodeCount - nodes.size() / (nodeCount - 1.)) * p + (1. - TAU) * w);

//            System.out.println("\t\tp = " + p);
//            System.out.println("\t\tw = " + w);
//            System.out.println("\t\twij = " + wij);
//            System.out.println("\t\tq = " + q);
        }


        @Override
        public String toString() {
            return nodes.toString();
        }
    }
}
