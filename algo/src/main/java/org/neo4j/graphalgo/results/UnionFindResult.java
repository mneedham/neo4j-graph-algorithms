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

import org.neo4j.graphalgo.impl.DSSResult;

/**
 * @author mknblch
 */
public class UnionFindResult extends CommunityResult {

    public UnionFindResult(long loadMillis, long computeMillis, long writeMillis, long postProcessingMillis, long nodes, long communityCount, long iterations, boolean convergence, long p99, long p95, long p90, long p75, long p50, long p25, long p10, long p05, long p01, long[] biggestCommunities) {
        super(loadMillis, computeMillis, writeMillis, postProcessingMillis, nodes, communityCount, iterations, convergence, p99, p95, p90, p75, p50, p25, p10, p05, p01, biggestCommunities);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends CommunityResult.AbstractCommunityResultBuilder<UnionFindResult> {

        public UnionFindResult build(DSSResult result) {
            if (result.isHuge) {
                return build(result.hugeStruct.capacity(), result.hugeStruct::find);
            } else {
                return build(result.struct.capacity(), l -> (long) result.struct.find((int) l));
            }
        }

        @Override
        protected UnionFindResult build(long loadMillis, long computeMillis, long writeMillis, long postProcessingMillis, long nodeCount, long communityCount, long iterations, boolean convergence, long p99, long p95, long p90, long p75, long p50, long p25, long p10, long p05, long p01, long[] top3Communities) {
            return new UnionFindResult(loadMillis, computeMillis, writeMillis, postProcessingMillis, communityCount, communityCount, iterations, convergence, p99, p95, p90, p75, p50, p25, p10, p05, p01, top3Communities);
        }
    }
}
