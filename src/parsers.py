from typing import List


class ShortestPathParser:
    """
    Contains methods that extract various information from the shortest path result
    from the Dijkstra's algorithm query.

    Attibutes:
    spath_data (List[dict])
        - The output from the Dijkstar's algorithm query.
    """
    def __init__(self, spath_data: List[dict]) -> None:
        self.spath_data = spath_data

    def _extract_connection_durations(self) -> List[float]:
        """
        Extracts the durations between each connection/interchange returned by the
        Dijkstra's algorith query.

        Parameters:
        costs (List[float])
            - The costs list in the data returned by the Dijkstra's algorithm query.
        """
        costs = self.spath_data[0]["costs"]
        durations = [costs[i + 1] - costs[i] for i in range(len(costs) - 1)]
        return durations

    def extract_shortest_path_summary(self) -> List[str]:
        """
        Parses the shortest path data returned by the Dijkstra's algorthim query into a
        natural language summary of the journey. Each step of the jounrey is returned as
        a string in a list

        Parameters:
        shortest_path (List[dict])
            - The data returned by the Dijkstra's shortest path algorithm.
        """
        spath_summary = []
        path = self.spath_data[0]["path"]
        durations = self._extract_connection_durations()

        for i, node in enumerate(path):
            current_line = node["line"]
            current_station = node["name"]

            # Get on first station
            if i == 0:
                spath_summary.append(
                    f"Get on the {current_line} line at {current_station}."
                )
            # Determine if any station after first station is an interchange or
            # connection
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

        total_duration = self.spath_data[0]["totalCost"]
        spath_summary.append(
            f"Total duration for the jouney is {total_duration} minutes."
        )
        return spath_summary
