MATCH (source:Station {name: $station_from}), (target:Station {name: $station_to})
CALL gds.shortestPath.dijkstra.stream(
    $graph_name,
    {
        sourceNode: source,
        targetNode: target,
        relationshipWeightProperty: 'duration'
    }
)
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN
    gds.util.asNode(sourceNode).name AS sourceNodeName,
    gds.util.asNode(targetNode).name AS targetNodeName,
    totalCost,
    [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames,
    costs,
    nodes(path) AS path,