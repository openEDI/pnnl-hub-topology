import logging
import networkx as nx
from oedisi.types.data_types import (
    IncidenceList,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


def get_switches(graph: nx.Graph):
    switches = []
    for u, v, a in graph.edges(data=True):
        if "SWITCH" == a["tag"]:
            switches.append((u, v, a))
    return switches


def area_disconnects(graph: nx.Graph):
    n_max = 5
    switches = get_switches(graph)
    areas = disconnect_areas(graph, switches)
    area_cnt = [area.number_of_nodes() for area in areas]
    min_n = [area.number_of_nodes() for area in areas]
    min_n.sort(reverse=True)
    min_n = min(min_n[0:n_max])
    z_area = zip(area_cnt, areas)
    z_area = sorted(z_area, key=lambda v: v[0])

    closed = []
    cnt = 0
    for n, area in z_area:
        if n < 2 or n < min_n or cnt > n_max:
            for u, v, a in switches:
                if area.has_node(u) or area.has_node(v):
                    closed.append((u, v, a))
            continue
        cnt += 1

    open = [(u, v, a) for u, v, a in switches if (u, v, a) not in closed]
    return open


def disconnect_areas(graph: nx.Graph, switches) -> list[nx.Graph]:
    graph.remove_edges_from(switches)

    areas = []
    for c in nx.connected_components(graph):
        areas.append(graph.subgraph(c).copy())
    return areas


def generate_graph(inc: IncidenceList, slack_bus: str) -> nx.Graph:
    graph = nx.Graph()
    for src, dst, id in zip(inc.from_equipment, inc.to_equipment, inc.ids):
        if "OPEN" in src or "OPEN" in dst:
            continue
        if src == dst:
            continue

        if "." in src:
            src, _ = src.split(".", 1)
        if "." in dst:
            dst, _ = dst.split(".", 1)

        eq = "LINE"
        if ("sw" in id or "fuse" in id) and "padswitch" not in id:
            eq = "SWITCH"
        if "tr" in id or "reg" in id or "xfm" in id:
            eq = "XFMR"
        graph.add_edge(src, dst, name=f"{src}_{dst}", tag=eq, id=f"{id}")

    for c in nx.connected_components(graph):
        if slack_bus in c:
            return graph.subgraph(c).copy()

    return graph


def get_area_source(graph: nx.Graph, slack_bus: str, switches):
    paths = {}
    for u, v, a in switches:
        paths[len(nx.shortest_path(graph, slack_bus, u))] = (u, v, a)
    source = min(paths, key=lambda k: paths[k])
    return paths[source]


def reconnect_area_switches(areas: list[nx.Graph], switches):
    for area in areas:
        for u, v, a in switches:
            if area.has_node(u) or area.has_node(v):
                area.add_edge(u, v, **a)
    return areas
