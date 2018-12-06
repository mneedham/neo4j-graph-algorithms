package org.neo4j.graphalgo.impl.infomap;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectScatterMap;
import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.sources.ShuffledNodeIterator;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.BitSet;
import java.util.function.Consumer;

import static org.neo4j.graphalgo.impl.infomap.MapEquation.TAU;
import static org.neo4j.graphalgo.impl.infomap.MapEquationAlgorithm.plogp;

/**
 * @author mknblch
 */
public class MapEquationV2 extends Algorithm<MapEquationV2> implements MapEquationAlgorithm {

    public static final Direction D = Direction.OUTGOING;
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
            d[0] += plogp(pageRanks.weightOf(node));
            modules.put(node, new Module(node));
            return true;
        });
        this.totalGraphPageRankEntropy = d[0];
    }

    public void compute() {
        boolean changed = false;
        for (int i = 0; i < 10; i++) {
            changed = false;
            for (IntObjectCursor<Module> cursor : modules) {
                final double sumQi = sumQi();
                final double mdl = getMDL();
                final Module module = cursor.value;
//                System.out.println("module.moduleIndex = " + module.moduleIndex);
                final Pointer.GenericPointer<Module> best = Pointer.wrap(null);
                final Pointer.DoublePointer m = Pointer.wrap(mdl);

                forEachNeighboringModule(module, other -> {

                    final double v = mdl + delta(module, other, sumQi);
                    if (v > 0 && v < m.v) {
                        m.v = v;
                        best.v = other;
                    }
                });

                if (null == best.v) {
                    continue;
                }

                changed = true;
                merge(module.moduleIndex, best.v.moduleIndex);
            }

            if (!changed) {
                break;
            }
        }
    }


    void forEachNeighboringModule(Module m, Consumer<Module> consumer) {

        final BitSet bitSet = new BitSet(nodeCount);

        m.nodes.forEach((IntProcedure) node -> {
//            System.out.println("node = " + node);
            graph.forEachOutgoing(node, (s, t, r) -> {
                final int community = communities[t];
                if (communities[s] == community) {
                    return true;
                }
                if (bitSet.get(community)) {
                    return true;
                }
                bitSet.set(community);
                consumer.accept(modules.get(community));
                return true;
            });
        });

    }


    public void merge(int modA, int modB) {

        if (modA == modB) {
            return;
        }

        final Module module = modules.get(modA);
        final double delta = delta(module, modules.get(modB), sumQi());
        final double mdl = getMDL();


        final Module other = modules.remove(modB);
        final String targetSet = module.nodes.toString();
        final String sourceSet = other.nodes.toString();
        module.merge(other);

        final double realMdl = getMDL();

        System.out.printf("move %12s -> %12s | deltaL = %.3f | calc mdl %2.3f | real mdl %2.3f => %s%n", sourceSet, targetSet, delta, delta + mdl, realMdl, Arrays.toString(communities));

    }

    public double delta(Module a, Module b, double sumQi) {


        sumQi = sumQi();

        final IntSet merge = new IntScatterSet();
        a.nodes.forEach((IntProcedure) merge::add);
        b.nodes.forEach((IntProcedure) merge::add);

        double ni = a.nodes.size() + b.nodes.size();
        double pi = a.p + b.p;
        double wi = a.w + b.w - Wj_k(a, b);
        double qi = TAU * ((double) nodeCount - ni) / (nodeCount - 1.) * pi + (1. - TAU) * wi;

        return plogp(qi - a.q - b.q + sumQi)
                - plogp(sumQi)
                - 2 * plogp(qi)
                + 2 * plogp(a.q)
                + 2 * plogp(b.q)
                + plogp(pi + qi)
                - plogp(a.p + a.q)
                - plogp(b.p + b.q)
                ;
    }

    double sumQi() {
        double qi = .0;
        for (IntObjectCursor<Module> module : modules) {
            qi += module.value.q;
        }
        return qi;
    }


    @Override
    public double getMDL() {
        double sumQi = 0;
        double sumplogpQi = 0;
        double sumPQi = 0;

        for (IntObjectCursor<Module> cursor : modules) {
            final Module module = cursor.value;
            sumQi += module.q;
            sumplogpQi += plogp(module.q);
            sumPQi += plogp(module.p + module.q);
        }

        return plogp(sumQi) - 2 * sumplogpQi - totalGraphPageRankEntropy + sumPQi;
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


    double interModuleWeight(Module j, Module k) {
        final Pointer.DoublePointer w = Pointer.wrap(.0);
        j.nodes.forEach((IntProcedure) c -> {
            graph.forEachOutgoing(c, (s, t, r) -> {
                if (k.nodes.contains(t)) {
                    w.v += (weights.weightOf(s, t));
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
//            graph.forEachRelationship(startNode, D, (s, t, r) -> {
//                if (s == t) {
//                    return true;
//                }
//                initW.v += 1. / graph.degree(t, D); // weights.weightOf(s, t);
//                return true;
//            });
            w = p; //initW.v;
            q = TAU * p + (1. - TAU) * w;
        }

        public void merge(Module other) {


            final IntSet merge = new IntScatterSet();
            this.nodes.forEach((IntProcedure) merge::add);
            other.nodes.forEach((IntProcedure) merge::add);

            p += other.p;
            w = w + other.w - Wj_k(this, other);
            q = TAU * (((double) nodeCount - merge.size()) / (nodeCount - 1.)) * p + (1. - TAU) * w;

            other.nodes.forEach((IntProcedure) nodes::add);
            nodes.forEach((IntProcedure) node -> communities[node] = moduleIndex);

        }
    }
}
