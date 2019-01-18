package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import com.carrotsearch.hppc.sorting.IndirectComparator;
import com.carrotsearch.hppc.sorting.IndirectSort;
import org.HdrHistogram.Histogram;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author mknblch
 */
public class CommunityResult {

    /*

    YIELD

     */

    public final long loadDuration;
    public final long evalDuration;
    public final long writeDuration;
    public final long nodes;
    public final long communityCount;
    public final long iterations;
    public final boolean convergence;
    public final long p99;
    public final long p95;
    public final long p90;
    public final long p75;
    public final long p50;
    public final int[] biggestCommunities;

    CommunityResult(long loadDuration,
                    long evalDuration,
                    long writeDuration,
                    long nodes,
                    long communityCount,
                    long iterations,
                    boolean convergence,
                    long p99,
                    long p95,
                    long p90,
                    long p75,
                    long p50,
                    int[] biggestCommunities) {
        this.loadDuration = loadDuration;
        this.evalDuration = evalDuration;
        this.writeDuration = writeDuration;
        this.nodes = nodes;
        this.communityCount = communityCount;
        this.iterations = iterations;
        this.convergence = convergence;
        this.p99 = p99;
        this.p95 = p95;
        this.p90 = p90;
        this.p75 = p75;
        this.p50 = p50;
        this.biggestCommunities = biggestCommunities;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<CommunityResult> {

        private long nodes = 1;
        private long iterations = 1;
        private int[] communities = new int[]{};
        private boolean convergence = false;

        public Builder withIterations(long iterations) {
            this.iterations = iterations;
            return this;
        }

        public Builder withConvergence(boolean convergence) {
            this.convergence = convergence;
            return this;
        }

        public Builder withNodes(long nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder withCommunities(int[] communities) {
            this.communities = communities;
            return this;
        }

        private static int[] mapSortTopN(IntIntHashMap map, int topN) {
            return mapSortTopN_(topN, map.keys, new HppcMapComparator(map));
        }

        public static int[] mapSortTopN_dense(IntIntHashMap map, int topN) {
            int size = map.size();
            int[] keys = new int[size];
            int[] values = new int[size];
            Iterator<IntIntCursor> cursor = map.iterator();
            for (int index = 0; cursor.hasNext(); ++index) {
                IntIntCursor entry = cursor.next();
                keys[index] = entry.key;
                values[index] = entry.value;
            }

            return mapSortTopN_(topN, keys, new IndirectComparator.DescendingIntComparator(values));
        }

        private static int[] mapSortTopN_(int topN, int[] keys, IndirectComparator comp) {
            int[] sortedKeys = IndirectSort.mergesort(0, keys.length, comp);
            topN = Math.min(topN, keys.length);
            for (int i = 0; i < topN; i++) {
                sortedKeys[i] = keys[sortedKeys[i]];
            }
            return Arrays.copyOf(sortedKeys, topN);
        }

        @Override
        public CommunityResult build() {
            // evaluate count and biggest communities
            final IntIntHashMap map = new IntIntHashMap();
            final Histogram histogram = new Histogram(0);

            for (int i = 0; i < communities.length; i++) {
                map.addTo(communities[i], 1);
                histogram.recordValue(communities[i]);
            }

            return new CommunityResult(
                    loadDuration,
                    evalDuration,
                    writeDuration,
                    nodes,
                    map.size(),
                    iterations, convergence,
                    histogram.getValueAtPercentile(.99),
                    histogram.getValueAtPercentile(.95),
                    histogram.getValueAtPercentile(.90),
                    histogram.getValueAtPercentile(.75),
                    histogram.getValueAtPercentile(.50),
                    mapSortTopN(map, 3));
        }
    }

}