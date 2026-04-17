"""
Topology splitter: splits an oedisi Topology into per-switch-area sub-topologies.

Each area is determined by the switch boundaries identified in the network graph.
All topology data (admittance, injections, voltages, incidences) is filtered to
only include entries whose buses belong to the area. Boundary switches are
included in both adjacent areas.
"""

import copy
from typing import List, Optional

from oedisi.types.data_types import (
    AdmittanceSparse,
    Injection,
    IncidenceList,
    Topology,
)
from pydantic import BaseModel

try:
    from .adapter import (
        area_disconnects,
        disconnect_areas,
        generate_graph,
        get_area_source,
        reconnect_area_switches,
    )
except ImportError:
    from adapter import (  # type: ignore[no-redef]
        area_disconnects,
        disconnect_areas,
        generate_graph,
        get_area_source,
        reconnect_area_switches,
    )


class AreaTopology(BaseModel):
    """A complete topology for a single switch area, plus metadata."""

    topology: Topology
    area_index: int
    switch_ids: List[str]
    source_bus: str


def _base_bus(node_id: str) -> str:
    """Return the base bus name by stripping the phase suffix (e.g. '13.1' → '13')."""
    return node_id.split(".", 1)[0] if "." in node_id else node_id


def _filter_incidences(
    incidences: Optional[IncidenceList], area_nodes: set
) -> Optional[IncidenceList]:
    if incidences is None:
        return None

    froms, tos, ids, types = [], [], [], []
    eq_types = incidences.equipment_type

    for i, (f, t, eid) in enumerate(
        zip(incidences.from_equipment, incidences.to_equipment, incidences.ids)
    ):
        if _base_bus(f) in area_nodes and _base_bus(t) in area_nodes:
            froms.append(f)
            tos.append(t)
            ids.append(eid)
            if eq_types is not None:
                types.append(eq_types[i])

    return IncidenceList(
        from_equipment=froms,
        to_equipment=tos,
        ids=ids,
        equipment_type=types if eq_types is not None else None,
    )


def _filter_admittance(
    admittance: AdmittanceSparse, area_nodes: set
) -> AdmittanceSparse:
    froms, tos, adm_list, types = [], [], [], []
    eq_types = admittance.equipment_type

    for i, (f, t, adm) in enumerate(
        zip(
            admittance.from_equipment,
            admittance.to_equipment,
            admittance.admittance_list,
        )
    ):
        if _base_bus(f) in area_nodes and _base_bus(t) in area_nodes:
            froms.append(f)
            tos.append(t)
            adm_list.append(adm)
            if eq_types is not None:
                types.append(eq_types[i])

    return AdmittanceSparse(
        from_equipment=froms,
        to_equipment=tos,
        admittance_list=adm_list,
        equipment_type=types if eq_types is not None else None,
        units=admittance.units,
    )


def _filter_equipment_node_array(array, area_nodes: set):
    """Filter any EquipmentNodeArray subclass (PowersReal, PowersImaginary, etc.)."""
    indices = [i for i, nid in enumerate(array.ids) if _base_bus(nid) in area_nodes]

    kwargs = {
        "ids": [array.ids[i] for i in indices],
        "values": [array.values[i] for i in indices],
        "units": array.units,
        "accuracy": [array.accuracy[i] for i in indices] if array.accuracy else None,
        "bad_data_threshold": (
            [array.bad_data_threshold[i] for i in indices]
            if array.bad_data_threshold
            else None
        ),
        "time": array.time,
        "equipment_ids": [array.equipment_ids[i] for i in indices],
    }
    return type(array)(**kwargs)


def _filter_node_array(array, area_nodes: set):
    """Filter any plain node array (CurrentsReal/Imaginary, ImpedanceReal/Imaginary)."""
    # These use node_ids not ids as the bus key
    if hasattr(array, "node_ids") and array.node_ids:
        indices = [
            i for i, nid in enumerate(array.node_ids) if _base_bus(nid) in area_nodes
        ]
        return type(array)(
            ids=[array.ids[i] for i in indices],
            values=[array.values[i] for i in indices],
            node_ids=[array.node_ids[i] for i in indices],
        )
    # Empty array — return as-is
    return array


def _filter_bus_array(array, area_nodes: set):
    """Filter a BusArray subclass (VoltagesMagnitude, VoltagesAngle)."""
    indices = [i for i, nid in enumerate(array.ids) if _base_bus(nid) in area_nodes]

    return type(array)(
        ids=[array.ids[i] for i in indices],
        values=[array.values[i] for i in indices],
        units=array.units,
        accuracy=[array.accuracy[i] for i in indices] if array.accuracy else None,
        bad_data_threshold=(
            [array.bad_data_threshold[i] for i in indices]
            if array.bad_data_threshold
            else None
        ),
        time=array.time,
    )


def _filter_injection(injection: Injection, area_nodes: set) -> Injection:
    return Injection(
        current_real=_filter_node_array(injection.current_real, area_nodes),
        current_imaginary=_filter_node_array(injection.current_imaginary, area_nodes),
        power_real=_filter_equipment_node_array(injection.power_real, area_nodes),
        power_imaginary=_filter_equipment_node_array(
            injection.power_imaginary, area_nodes
        ),
        impedance_real=_filter_node_array(injection.impedance_real, area_nodes),
        impedance_imaginary=_filter_node_array(
            injection.impedance_imaginary, area_nodes
        ),
    )


def _upstream_bus(area_nodes_before_reconnect: set, u: str, v: str) -> str:
    """Given a switch edge (u, v), return the endpoint that is NOT in the area."""
    u_in = u in area_nodes_before_reconnect
    v_in = v in area_nodes_before_reconnect
    if v_in and not u_in:
        return u
    if u_in and not v_in:
        return v
    # Both or neither — fall back to u (caller should use get_area_source to pick)
    return u


def split_topology(topology: Topology) -> List[AreaTopology]:
    """Split a Topology into per-switch-area sub-topologies.

    Each area is defined by the switch boundaries in the network. Boundary
    switches are included in both adjacent areas. The returned list of
    ``AreaTopology`` objects each contain:

    - ``topology``: a complete, filtered ``Topology`` for the area
    - ``area_index``: zero-based index
    - ``switch_ids``: boundary switch IDs that define this area's boundary
    - ``source_bus``: base bus name serving as the area's source/slack
    """
    slack_bus_phases = topology.slack_bus
    slack_bus_base = _base_bus(slack_bus_phases[0]) if slack_bus_phases else ""

    # Build the full network graph
    if topology.incidences is None:
        exit(1)

    G = generate_graph(topology.incidences, slack_bus_base)

    # Identify boundary switches and split into disconnected areas
    g_for_boundaries = copy.deepcopy(G)
    g_for_split = copy.deepcopy(G)

    boundaries = area_disconnects(g_for_boundaries)
    areas_disconnected = disconnect_areas(g_for_split, boundaries)

    # Record node sets before boundary switches are re-added (used for upstream bus detection)
    area_nodes_before_reconnect = [set(area.nodes()) for area in areas_disconnected]

    areas = reconnect_area_switches(areas_disconnected, boundaries)

    result: List[AreaTopology] = []

    for i, area in enumerate(areas):
        pre_reconnect_nodes = area_nodes_before_reconnect[i]
        area_all_nodes = set(area.nodes())

        # Determine which boundary switches touch this area
        area_boundary_edges = [
            (u, v, a) for u, v, a in boundaries if area.has_edge(u, v)
        ]
        area_switch_ids = [a["id"] for _, _, a in area_boundary_edges]

        # Determine the source/slack bus for this area
        if area.has_node(slack_bus_base):
            source_bus = slack_bus_base
            area_slack_bus = list(slack_bus_phases)
        else:
            su, sv, _ = get_area_source(G, slack_bus_base, area_boundary_edges)
            upstream = _upstream_bus(pre_reconnect_nodes, su, sv)
            source_bus = upstream
            # Collect all phase variants of the upstream bus from base_voltage_magnitudes
            if topology.base_voltage_magnitudes:
                area_slack_bus = [
                    vid
                    for vid in topology.base_voltage_magnitudes.ids
                    if _base_bus(vid) == upstream
                ]
            else:
                area_slack_bus = [upstream]

        # Build the filtered sub-topology
        if not isinstance(topology.admittance, AdmittanceSparse):
            exit(1)

        area_topology = Topology(
            admittance=_filter_admittance(topology.admittance, area_all_nodes),
            injections=_filter_injection(topology.injections, area_all_nodes),
            incidences=_filter_incidences(topology.incidences, area_all_nodes),
            base_voltage_magnitudes=(
                _filter_bus_array(topology.base_voltage_magnitudes, area_all_nodes)
                if topology.base_voltage_magnitudes
                else None
            ),
            base_voltage_angles=(
                _filter_bus_array(topology.base_voltage_angles, area_all_nodes)
                if topology.base_voltage_angles
                else None
            ),
            slack_bus=area_slack_bus,
        )

        result.append(
            AreaTopology(
                topology=area_topology,
                area_index=i,
                switch_ids=area_switch_ids,
                source_bus=source_bus,
            )
        )

    return result
