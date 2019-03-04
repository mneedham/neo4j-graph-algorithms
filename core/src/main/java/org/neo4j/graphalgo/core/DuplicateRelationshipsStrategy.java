package org.neo4j.graphalgo.core;

public enum DuplicateRelationshipsStrategy {
    NONE {
        public double merge(double runningTotal, double weight) {
            throw new UnsupportedOperationException();
        }
    },
    SKIP {
        public double merge(double runningTotal, double weight) {
            return runningTotal;
        }
    },
    SUM {
        public double merge(double runningTotal, double weight) {
            return runningTotal + weight;
        }
    },
    MIN {
        public double merge(double runningTotal, double weight) {
            return Math.min(runningTotal, weight);
        }
    },
    MAX {
        public double merge(double runningTotal, double weight) {
            return Math.max(runningTotal, weight);
        }
    };

    public abstract double merge(double runningTotal, double weight);

}
