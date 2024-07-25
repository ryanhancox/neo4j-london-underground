MATCH (source:Station {name: $start_station}), (target:Station {name: $end_station})
CALL gds.shortestPath.dijkstra.stream(
    'tube_graph',   // name of graph
    {
        sourceNode: source,
        targetNode: target,
        relationshipWeightProperty: $metric
    }
)
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN
    index,
    gds.util.asNode(sourceNode).name AS sourceNodeName,
    gds.util.asNode(targetNode).name AS targetNodeName,
    totalCost,
    [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames,
    costs,
    nodes(path) as path,
    COLLECT {
    WITH nodes(path) as pathNodes
    UNWIND range(0,size(pathNodes)-2) as i
    WITH pathNodes[i] as a, pathNodes[i+1] as b
    MATCH (a)-[r:CONNECTION]->(b)
    RETURN r
    } as connections
ORDER BY index;  