/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.results;

import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.PagedDisjointSetStruct;
import org.neo4j.graphalgo.impl.DSSResult;

/**
 * @author mknblch
 */
public class UnionFindResult extends CommunityResult {

    UnionFindResult(long loadMillis, long computeMillis, long writeMillis, long nodes, long communityCount, long iterations, boolean convergence, long p99, long p95, long p90, long p75, long p50, long[] biggestCommunities) {
        super(loadMillis, computeMillis, writeMillis, nodes, communityCount, iterations, convergence, p99, p95, p90, p75, p50, biggestCommunities);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends CommunityResult.AbstractCommunityBuilder<CommunityResult> {


        public Builder withDSSResult(DSSResult result) {
            if (result.isHuge) {
                withStruct(result.hugeStruct);
            } else {
                withStruct(result.struct);
            }
            return this;
        }

        public Builder withStruct(DisjointSetStruct setStruct) {
            withCommunities(setStruct.capacity(), i -> (long) setStruct.find((int) i));
            return this;
        }

        public Builder withStruct(PagedDisjointSetStruct setStruct) {
            withCommunities(setStruct.capacity(), setStruct::find);
            return this;
        }

        @Override
        public CommunityResult build() {

            return new UnionFindResult(
                    loadDuration,
                    evalDuration,
                    writeDuration,
                    nodes,
                    communityMap.size(),
                    iterations, convergence,
                    histogram.getValueAtPercentile(.99),
                    histogram.getValueAtPercentile(.95),
                    histogram.getValueAtPercentile(.90),
                    histogram.getValueAtPercentile(.75),
                    histogram.getValueAtPercentile(.50),
                    top3);
        }
    }


}
