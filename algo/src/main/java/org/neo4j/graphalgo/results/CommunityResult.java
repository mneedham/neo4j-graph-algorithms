package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.LongLongHashMap;
import org.HdrHistogram.Histogram;
import org.neo4j.graphalgo.core.utils.ProgressTimer;

import java.util.function.LongFunction;

/**
 * @author mknblch
 */
@SuppressWarnings("WeakerAccess")
public class CommunityResult {

    /*
        YIELD loadMillis, computeMillis, writeMillis, nodes, communityCount, iterations, convergence, p99, p95, p90, p75, p50, p25, p10, p05, p01, top3
     */

    public final long loadMillis;
    public final long computeMillis;
    public final long postProcessingMillis;
    public final long writeMillis;
    public final long nodes;
    public final long communityCount;
    public final long iterations;
    public final boolean convergence;
    public final long p99;
    public final long p95;
    public final long p90;
    public final long p75;
    public final long p50;
    public final long p25;
    public final long p10;
    public final long p05;
    public final long p01;
    public final long[] top3;

    public CommunityResult(long loadMillis,
                           long computeMillis,
                           long writeMillis,
                           long postProcessingMillis, long nodes,
                           long communityCount,
                           long iterations,
                           boolean convergence,
                           long p99,
                           long p95,
                           long p90,
                           long p75,
                           long p50,
                           long p25,
                           long p10,
                           long p05,
                           long p01,
                           long[] biggestCommunities) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.postProcessingMillis = postProcessingMillis;
        this.nodes = nodes;
        this.communityCount = communityCount;
        this.iterations = iterations;
        this.convergence = convergence;
        this.p99 = p99;
        this.p95 = p95;
        this.p90 = p90;
        this.p75 = p75;
        this.p50 = p50;
        this.p25 = p25;
        this.p10 = p10;
        this.p05 = p05;
        this.p01 = p01;
        this.top3 = biggestCommunities;
    }

    /**
     * Helper class for creating Builders for community algo results
     * @param <T> concrete type of the result
     */
    public static abstract class AbstractCommunityResultBuilder<T extends CommunityResult> {

        private long loadDuration = -1;
        private long evalDuration = -1;
        private long writeDuration = -1;
        private long iterations = -1;
        private boolean convergence = false;

        public AbstractCommunityResultBuilder<T> withLoadDuration(long loadDuration) {
            this.loadDuration = loadDuration;
            return this;
        }

        public AbstractCommunityResultBuilder<T> withEvalDuration(long evalDuration) {
            this.evalDuration = evalDuration;
            return this;
        }

        public AbstractCommunityResultBuilder<T> withWriteDuration(long writeDuration) {
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


        /**
         * number of iterations the algorithm did in total. Can be omitted.
         * @param iterations
         * @return
         */
        public AbstractCommunityResultBuilder<T> withIterations(long iterations) {
            this.iterations = iterations;
            return this;
        }

        /**
         * should be set to true if the algorithm did (and is able to) converge to an
         * optimum. Should be set to false if the algorithm ran into an artificial limit
         * (like maximum iterations without convergence). Can be omitted if the algo
         * does not have such thing
         *
         * @param convergence
         * @return
         */
        public AbstractCommunityResultBuilder<T> withConvergence(boolean convergence) {
            this.convergence = convergence;
            return this;
        }

        // do it
        protected abstract T build(
                long loadMillis, long computeMillis, long writeMillis, long postProcessingMillis,
                long nodeCount, long communityCount, long iterations, boolean convergence,
                long p99, long p95, long p90, long p75, long p50, long p25, long p10, long p05, long p01,
                long[] top3Communities
        );

        /**
         * build result
         * @param nodes number of nodes in the graph
         * @param fun nodeId to communityId mapping function
         * @return result
         */
        public T build(long nodes, LongFunction<Long> fun) {

            final long[] top3 = new long[]{-1L, -1L, -1L};
            final Histogram histogram = new Histogram(2);
            final LongLongHashMap communityMap = new LongLongHashMap();

            final ProgressTimer timer = ProgressTimer.start();
            for (int i = 0; i < nodes; i++) {
                // map to community id
                final long r = fun.apply(i);
                // aggregate community size
                communityMap.addTo(r, 1);
                // fill histogram
                histogram.recordValue(r);
                // eval top 3 communities
                if (r > top3[0]) {
                    top3[2] = top3[1];
                    top3[1] = top3[0];
                    top3[0] = r;
                } else if (r > top3[1]) {
                    top3[2] = top3[1];
                    top3[1] = r;
                } else if (r > top3[2]) {
                    top3[2] = r;
                }
            }
            timer.stop();

            return build(
                    loadDuration,
                    evalDuration,
                    writeDuration,
                    timer.getDuration(),
                    nodes,
                    communityMap.size(),
                    iterations,
                    convergence,
                    histogram.getValueAtPercentile(.99),
                    histogram.getValueAtPercentile(.95),
                    histogram.getValueAtPercentile(.9),
                    histogram.getValueAtPercentile(.75),
                    histogram.getValueAtPercentile(.5),
                    histogram.getValueAtPercentile(.25),
                    histogram.getValueAtPercentile(.1),
                    histogram.getValueAtPercentile(.05),
                    histogram.getValueAtPercentile(.01),
                    top3
            );
        }

    }
}
