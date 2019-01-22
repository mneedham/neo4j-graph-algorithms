package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.HdrHistogram.Histogram;
import org.neo4j.graphalgo.core.utils.ProgressTimer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.LongFunction;

/**
 * unified community algo result
 *
 * YIELD loadMillis, computeMillis, writeMillis, nodes, communityCount, iterations, convergence, p99, p95, p90, p75, p50, p25, p10, p05, p01, top3
 *
 * @author mknblch
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractCommunityResult {

    public final long loadMillis;
    public final long computeMillis;
    public final long postProcessingMillis;
    public final long writeMillis;
    public final long nodes;
    public final long communityCount;
    public final long p99;
    public final long p95;
    public final long p90;
    public final long p75;
    public final long p50;
    public final long p25;
    public final long p10;
    public final long p05;
    public final long p01;
    public final List<Long> top3;

    public AbstractCommunityResult(long loadMillis,
                                   long computeMillis,
                                   long writeMillis,
                                   long postProcessingMillis, long nodes,
                                   long communityCount,
                                   long p99,
                                   long p95,
                                   long p90,
                                   long p75,
                                   long p50,
                                   long p25,
                                   long p10,
                                   long p05,
                                   long p01,
                                   List<Long> top) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.postProcessingMillis = postProcessingMillis;
        this.nodes = nodes;
        this.communityCount = communityCount;
        this.p99 = p99;
        this.p95 = p95;
        this.p90 = p90;
        this.p75 = p75;
        this.p50 = p50;
        this.p25 = p25;
        this.p10 = p10;
        this.p05 = p05;
        this.p01 = p01;
        this.top3 = top;

    }

    /**
     * Helper class for creating Builders for community algo results
     */
    public static class CommunityResultBuilder<T extends AbstractCommunityResult> {

        protected long loadDuration = -1;
        protected long evalDuration = -1;
        protected long writeDuration = -1;

        public CommunityResultBuilder<T> withLoadDuration(long loadDuration) {
            this.loadDuration = loadDuration;
            return this;
        }

        public CommunityResultBuilder<T> withEvalDuration(long evalDuration) {
            this.evalDuration = evalDuration;
            return this;
        }

        public CommunityResultBuilder<T> withWriteDuration(long writeDuration) {
            this.writeDuration = writeDuration;
            return this;
        }

        /**
         * returns an AutoClosable which measures the time
         * until it gets closed. Saves the duration as loadMillis
         * @return
         */
        public ProgressTimer timeLoad() {
            return ProgressTimer.start(this::withLoadDuration);
        }

        /**
         * returns an AutoClosable which measures the time
         * until it gets closed. Saves the duration as evalMillis
         * @return
         */
        public ProgressTimer timeEval() {
            return ProgressTimer.start(this::withEvalDuration);
        }

        /**
         * returns an AutoClosable which measures the time
         * until it gets closed. Saves the duration as writeMillis
         * @return
         */
        public ProgressTimer timeWrite() {
            return ProgressTimer.start(this::withWriteDuration);
        }

        /**
         * evaluates loadMillis
         * @param runnable
         */
        public void timeLoad(Runnable runnable) {
            try (ProgressTimer timer = timeLoad()) {
                runnable.run();
            }
        }

        /**
         * evaluates comuteMillis
         * @param runnable
         */
        public void timeEval(Runnable runnable) {
            try (ProgressTimer timer = timeEval()) {
                runnable.run();
            }
        }

        /**
         * evaluates writeMillis
         * @param runnable
         */
        public void timeWrite(Runnable runnable) {
            try (ProgressTimer timer = timeWrite()) {
                runnable.run();
            }
        }

        protected T build(
                long loadMillis, long computeMillis, long writeMillis, long postProcessingMillis,
                long nodeCount, long communityCount,
                long p99, long p95, long p90, long p75, long p50, long p25, long p10, long p05, long p01,
                List<Long> top3Communities) {
            //noinspection unchecked
            return (T) new DefaultCommunityResult(loadDuration,
                    evalDuration,
                    writeDuration,
                    postProcessingMillis,
                    nodeCount,
                    communityCount,
                    p99,
                    p95,
                    p90,
                    p75,
                    p50,
                    p25,
                    p10,
                    p05,
                    p01,
                    top3Communities);
        }

        private static List<Long> top3(LongLongMap assignment) {

            // index of top 3 biggest communities
            final Long[] t3idx = new Long[]{-1L, -1L, -1L};
            // size of the top 3 communities
            final long[] top3 = new long[]{-1L, -1L, -1L};

            for (LongLongCursor cursor : assignment) {
                if (cursor.value > top3[0]) {
                    top3[2] = top3[1];
                    top3[1] = top3[0];
                    top3[0] = cursor.value;
                    t3idx[2] = t3idx[1];
                    t3idx[1] = t3idx[0];
                    t3idx[0] = cursor.key;
                } else if (cursor.value > top3[1]) {
                    top3[2] = top3[1];
                    top3[1] = cursor.value;
                    t3idx[2] = t3idx[1];
                    t3idx[1] = cursor.key;
                } else if (cursor.value > top3[2]) {
                    top3[2] = cursor.value;
                    t3idx[2] = cursor.key;
                }
            }

            return Arrays.asList(t3idx);
        }

        /**
         * build result
         * @param nodes number of nodes in the graph
         * @param fun nodeId to communityId mapping function
         * @return result
         */
        public T build(long nodes, LongFunction<Long> fun) {

            final Histogram histogram = new Histogram(2);
            final LongLongMap communityMap = new LongLongScatterMap();

            final ProgressTimer timer = ProgressTimer.start();
            for (int i = 0; i < nodes; i++) {
                // map to community id
                final long r = fun.apply(i);
                // aggregate community size
                communityMap.addTo(r, 1);
                // fill histogram
                histogram.recordValue(r);
            }
            timer.stop();

            return build(loadDuration,
                    evalDuration,
                    writeDuration,
                    timer.getDuration(),
                    nodes,
                    communityMap.size(),
                    histogram.getValueAtPercentile(.99),
                    histogram.getValueAtPercentile(.95),
                    histogram.getValueAtPercentile(.9),
                    histogram.getValueAtPercentile(.75),
                    histogram.getValueAtPercentile(.5),
                    histogram.getValueAtPercentile(.25),
                    histogram.getValueAtPercentile(.1),
                    histogram.getValueAtPercentile(.05),
                    histogram.getValueAtPercentile(.01),
                    top3(communityMap));
        }

        public AbstractCommunityResult buildEmpty() {
            return new DefaultCommunityResult(
                    loadDuration,
                    evalDuration,
                    writeDuration,
                    -1,
                    0,
                    0,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    Collections.emptyList());
        }

    }

    private static class DefaultCommunityResult extends AbstractCommunityResult {

        public DefaultCommunityResult(long loadMillis, long computeMillis, long writeMillis, long postProcessingMillis, long nodes, long communityCount, long p99, long p95, long p90, long p75, long p50, long p25, long p10, long p05, long p01, List<Long> biggestCommunities) {
            super(loadMillis, computeMillis, writeMillis, postProcessingMillis, nodes, communityCount, p99, p95, p90, p75, p50, p25, p10, p05, p01, biggestCommunities);
        }
    }
}
