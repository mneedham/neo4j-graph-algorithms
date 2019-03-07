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

public class CentralityScore {

    public final long nodeId;
    public final Double score;

    public CentralityScore(long nodeId, final Double score) {
        this.nodeId = nodeId;
        this.score = score;
    }

    // TODO: return number of relationships as well
    //  the Graph API doesn't expose this value yet
    public static final class Stats {
        public final long nodes, loadMillis, computeMillis, writeMillis;
        public final boolean write;
        public final String writeProperty;

        Stats(
                long nodes,
                long loadMillis,
                long computeMillis,
                long writeMillis,
                boolean write,
                String writeProperty) {
            this.nodes = nodes;
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.write = write;
            this.writeProperty = writeProperty;
        }

        public static final class Builder extends AbstractWriteBuilder<Stats> {
            private long nodes;
            private boolean write;
            private String writeProperty;

            public Builder withNodes(long nodes) {
                this.nodes = nodes;
                return this;
            }


            public Builder withWrite(boolean write) {
                this.write = write;
                return this;
            }

            public Builder withProperty(String writeProperty) {
                this.writeProperty = writeProperty;
                return this;
            }

            public CentralityScore.Stats build() {
                return new CentralityScore.Stats(
                        nodes,
                        loadDuration,
                        evalDuration,
                        writeDuration,
                        write,
                        writeProperty);
            }
        }
    }
}
