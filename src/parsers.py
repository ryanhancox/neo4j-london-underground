from typing import List


def _extract_connection_durations(shortest_path: List[dict]) -> List[float]:
    """
    Extracts the durations between each connection/interchange returned by the
    Dijkstra's algorith query.

    Parameters:
    costs (List[float])
        - The costs list in the data returned by the Dijkstra's algorithm query.
    """
    costs = shortest_path[0]["costs"]
    durations = [costs[i + 1] - costs[i] for i in range(len(costs) - 1)]
    return durations


def extract_shortest_path_summary(shortest_path: List[dict]) -> List[List[str]]:
    """
    Parses the shortest path data returned by the Dijkstra's algorthim query into a
    natural language summary of the journey. Each step of the jounrey is returned as
    a string in a list

    Parameters:
    shortest_path (List[dict])
        - The data returned by the Dijkstra's shortest path algorithm.
    """
    spath_summary = []
    path = shortest_path[0]["path"]
    durations = _extract_connection_durations(shortest_path)

    for i, node in enumerate(path):
        current_line = node["line"]
        current_station = node["name"]

        # Get on first station
        if i == 0:
            spath_summary.append(
                f"Get on the {current_line} line at {current_station}."
            )
        # Determine if any station after first station is an interchange or connection
        elif i > 0:
            prev_line = path[i - 1]["line"]
            prev_station = path[i - 1]["name"]
            if (current_station == prev_station) and (current_line != prev_line):
                spath_summary.append(
                    f"Change at {current_station} from the {prev_line} line to"
                    f" the {current_line} line ({durations[i-1]} minutes)."
                )
            else:
                spath_summary.append(
                    f"Continue on the {current_line} line to {current_station}"
                    f" ({durations[i-1]} minutes)."
                )

    total_duration = shortest_path[0]["totalCost"]
    spath_summary.append(
        f"Total duration for the jouney is {total_duration} minutes."
    )
    return spath_summary
