/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.LongLongMap;
import org.HdrHistogram.Histogram;

public class LabelPropagationStats {

    public static final LabelPropagationStats EMPTY = new LabelPropagationStats(
            0,
            0,
            0,
            0,
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
            -1,
            0,
            false,
            false,
            "<empty>",
            "<empty>", "<empty>");

    public final long loadMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long postProcessingMillis;

    public final long nodes;
    public final long communityCount;
    public final long iterations;
    public final boolean didConverge;

    public final long p1;
    public final long p5;
    public final long p10;
    public final long p25;
    public final long p50;
    public final long p75;
    public final long p90;
    public final long p95;
    public final long p99;
    public final long p100;

    public final String weightProperty;
    public final boolean write;
    public final String partitionProperty;
    public final String writeProperty;

    public LabelPropagationStats(long loadMillis, long computeMillis, long postProcessingMillis, long writeMillis, long nodes,
                                 long communityCount, long p100, long p99, long p95, long p90, long p75, long p50, long p25, long p10, long p5, long p1, long iterations, boolean write, boolean didConverge,
                                 String weightProperty, String partitionProperty, String writeProperty) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.postProcessingMillis = postProcessingMillis;
        this.writeMillis = writeMillis;
        this.nodes = nodes;
        this.communityCount = communityCount;
        this.p100 = p100;
        this.p99 = p99;
        this.p95 = p95;
        this.p90 = p90;
        this.p75 = p75;
        this.p50 = p50;
        this.p25 = p25;
        this.p10 = p10;
        this.p5 = p5;
        this.p1 = p1;
        this.iterations = iterations;
        this.write = write;
        this.didConverge = didConverge;
        this.weightProperty = weightProperty;
        this.partitionProperty = partitionProperty;
        this.writeProperty = writeProperty;
    }


    public static class Builder extends AbstractCommunityResultBuilder<LabelPropagationStats> {

        private long iterations = 0;
        private boolean didConverge = false;
        private String weightProperty;
        private String partitionProperty;
        private String writeProperty;

        public Builder iterations(final long iterations) {
            this.iterations = iterations;
            return this;
        }

        public Builder didConverge(final boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }

        public Builder weightProperty(final String weightProperty) {
            this.weightProperty = weightProperty;
            return this;
        }

        public Builder partitionProperty(final String partitionProperty) {
            this.partitionProperty = partitionProperty;
            return this;
        }
        public Builder writeProperty(final String writeProperty) {
            this.writeProperty = writeProperty;
            return this;
        }


        @Override
        protected LabelPropagationStats build(long loadMillis, long computeMillis, long writeMillis, long postProcessingMillis, long nodeCount, long communityCount, LongLongMap communitySizeMap, Histogram communityHistogram, boolean write) {
            return new LabelPropagationStats(
                    loadMillis,
                    computeMillis,
                    writeMillis,
                    postProcessingMillis,
                    nodeCount,
                    communityCount,
                    communityHistogram.getValueAtPercentile(100),
                    communityHistogram.getValueAtPercentile(99),
                    communityHistogram.getValueAtPercentile(95),
                    communityHistogram.getValueAtPercentile(90),
                    communityHistogram.getValueAtPercentile(75),
                    communityHistogram.getValueAtPercentile(50),
                    communityHistogram.getValueAtPercentile(25),
                    communityHistogram.getValueAtPercentile(10),
                    communityHistogram.getValueAtPercentile(5),
                    communityHistogram.getValueAtPercentile(1),
                    iterations,
                    write,
                    didConverge,
                    weightProperty,
                    partitionProperty,
                    writeProperty
            );
        }

    }
}
