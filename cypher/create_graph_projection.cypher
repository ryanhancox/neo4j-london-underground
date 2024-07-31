CALL gds.graph.project(
    'tube_graph', 
    'Station', 
    'CONNECTION',
    {relationshipProperties: $metric}
    )