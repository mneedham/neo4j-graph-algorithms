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
    private final double weightSum;

    public MapEquationV2(Graph graph, NodeWeights pageRanks, RelationshipWeights normalizedWeights) {
        this.graph = graph;
        this.pageRanks = pageRanks;
        this.weights = normalizedWeights;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.modules = new IntObjectScatterMap<>(nodeCount);
        this.communities = new int[nodeCount];
        Arrays.setAll(communities, i -> i);
        final double[] d = {0., .0};
        graph.forEachNode(node -> {
            d[0] += plogp(pageRanks.weightOf(node));
            graph.forEachRelationship(node, D, (s, t, r) -> {
                d[1] += weights.weightOf(s, t);
                return true;
            });
            modules.put(node, new Module(node));
            return true;
        });
        this.totalGraphPageRankEntropy = d[0];
        this.weightSum = d[1] / 2.;
    }


    static class Merge{
        final Module c1;
        final Module c2;
        final double dl;

        Merge(Module c1, Module c2, double dl) {
            this.c1 = c1;
            this.c2 = c2;
            this.dl = dl;
        }
    }

    boolean optimize() {

        final double sumQi = sumQi();
        final Pointer.DoublePointer m = Pointer.wrap(0.);

        final int[] best = {-1, -1};

        for (IntObjectCursor<Module> cursor : modules) {
            final Module module = cursor.value;

            forEachNeighboringModule(module, other -> {

                final double v = delta(module, other, sumQi);

                //System.out.printf(" try %10s <- %10s ? %f%n", module.nodes, other.nodes, v);

                if (v < m.v) {
                    m.v = v;
                    best[0] = module.moduleIndex;
                    best[1] = other.moduleIndex;
                }
            });
        }

        if (best[0] == -1) {
            return false;
        }

        merge(best[0], best[1]);
        return true;

    }

    public void compute() {
        boolean changed;
        for (int j = 0; j < 10; j++) {
            if (!optimize()) {
                break;
            }

        }
    }


    void forEachNeighboringModule(Module m, Consumer<Module> consumer) {

        final BitSet bitSet = new BitSet(nodeCount);

        m.nodes.forEach((IntProcedure) node -> {
            final int cs = communities[node];
            // for each connected node t to this module
            graph.forEachOutgoing(node, (s, t, r) -> {
                final int community = communities[t];
                // same community
                if (cs == community) {
                    return true;
                }
                // already visited
                if (bitSet.get(community)) {
                    return true;
                }
                // do visit
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
        final double mdl = getMDL();


        final double delta = delta(module, modules.get(modB), sumQi());
        // WARN dont remove before delta OR wrong output (calc still ok)
        final Module other = modules.remove(modB);
        final String targetSet = module.nodes.toString();
        final String sourceSet = other.nodes.toString();
        module.merge(other);

        final double realMdl = getMDL();

        System.out.printf("move %12s -> %12s | deltaL = %.3f | calc mdl %2.3f | real mdl %2.3f => %s%n", sourceSet, targetSet, delta, delta + mdl, realMdl, Arrays.toString(communities));

    }

    public double delta(Module a, Module b, double sumQi) {


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
            final double p = pageRanks.weightOf(c);
            graph.forEachOutgoing(c, (s, t, r) -> {
                if (k.nodes.contains(t)) {
                    w.v += p * weights.weightOf(s, t); // TODO check p
//                    w.v += weights.weightOf(s, t);
//                    w.v += weights.weightOf(s, t) + weights.weightOf(t, s);
                }
                return true;
            });
        });
        return w.v;
    }

    double Wj_k(Module j, Module k) {
        return (interModuleWeight(j, k) + interModuleWeight(k, j));
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
            final Pointer.DoublePointer sumW = Pointer.wrap(.0);
            // sum of all weights
            graph.forEachRelationship(startNode, D, (s, t, r) -> {
                if (s != t) {
                    sumW.v += weights.weightOf(s, t);
                }
                return true;
            });
            w = p * sumW.v;
            q = TAU * p + (1. - TAU) * w;

        }

        public void merge(Module other) {


            final IntSet merge = new IntScatterSet();
            this.nodes.forEach((IntProcedure) merge::add);
            other.nodes.forEach((IntProcedure) merge::add);

//            final int ni = nodes.size() + other.nodes.size(); //merge.size();
            final int ni = merge.size();

            p += other.p;
            w += other.w - Wj_k(this, other);
            q = TAU * (((double) nodeCount - ni) / (nodeCount - 1.)) * p + (1. - TAU) * w;

            other.nodes.forEach((IntProcedure) nodes::add);
            nodes.forEach((IntProcedure) node -> communities[node] = moduleIndex);

        }
    }
}
