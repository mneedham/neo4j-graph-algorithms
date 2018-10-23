// tag::create-sample-graph[]

MERGE (paper0:Paper {name:'Paper 0'})
MERGE (paper1:Paper {name:'Paper 1'})
MERGE (paper2:Paper {name:'Paper 2'})
MERGE (paper3:Paper {name:'Paper 3'})
MERGE (paper4:Paper {name:'Paper 4'})
MERGE (paper5:Paper {name:'Paper 5'})
MERGE (paper6:Paper {name:'Paper 6'})

MERGE (paper1)-[:CITES]->(paper0)

MERGE (paper2)-[:CITES]->(paper0)
MERGE (paper2)-[:CITES]->(paper1)

MERGE (paper3)-[:CITES]->(paper0)
MERGE (paper3)-[:CITES]->(paper1)
MERGE (paper3)-[:CITES]->(paper2)

MERGE (paper4)-[:CITES]->(paper0)
MERGE (paper4)-[:CITES]->(paper1)
MERGE (paper4)-[:CITES]->(paper2)
MERGE (paper4)-[:CITES]->(paper3)

MERGE (paper5)-[:CITES]->(paper1)
MERGE (paper5)-[:CITES]->(paper4)

MERGE (paper6)-[:CITES]->(paper1)
MERGE (paper6)-[:CITES]->(paper4)

// end::create-sample-graph[]

// tag::stream-sample-graph[]

CALL algo.articleRank.stream('Paper', 'CITES', {iterations:20, dampingFactor:0.85})
YIELD nodeId, score
RETURN algo.getNodeById(nodeId).name AS page,score
ORDER BY score DESC

// end::stream-sample-graph[]

// tag::write-sample-graph[]

CALL algo.articleRank('Paper', 'CITES',
  {iterations:20, dampingFactor:0.85, write: true,writeProperty:"pagerank"})
YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, writeProperty

// end::write-sample-graph[]

// tag::cypher-loading[]

CALL algo.articleRank(
  'MATCH (p:Paper) RETURN id(p) as id',
  'MATCH (p1:Paper)-[:CITES]->(p2:Paper) RETURN id(p1) as source, id(p2) as target',
  {graph:'cypher', iterations:5, write: true}
)

// end::cypher-loading[]


// tag::huge-projection[]

CALL algo.articleRank('Paper','CITES', {graph:'huge'})
YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, writeProperty;

// end::huge-projection[]
