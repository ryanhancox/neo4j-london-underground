MATCH (source:Station)-[c:CONNECTED_TO]-(target:Station)
WITH gds.graph.project(
    $graph_name,
    source,
    target,
    {relationshipProperties: c { .duration }}
) as g
RETURN g.graphName