package org.neo4j.graphalgo;

import org.neo4j.graphalgo.impl.results.*;

public enum Normalization {
    NONE {
        @Override
        public CentralityResult apply(CentralityResult scores) {
            return scores;
        }
    },
    MAX {
        @Override
        public CentralityResult apply(CentralityResult scores) {
            return new NormalizedCentralityResult(scores, (score, result) -> score / result.max());
        }
    },
    L1NORM {
        @Override
        public CentralityResult apply(CentralityResult scores) {
            return new NormalizedCentralityResult(scores, (score, result) -> score / result.l1Norm());
        }
    },
    L2NORM {
        @Override
        public CentralityResult apply(CentralityResult scores) {
            return new NormalizedCentralityResult(scores, (score, result) -> score / result.l2Norm());
        }
    };

    public abstract CentralityResult apply(CentralityResult scores);
}
