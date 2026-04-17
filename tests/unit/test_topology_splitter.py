"""Unit tests for topology_splitter module."""

import json
import os
import pytest

from oedisi.types.data_types import Topology

from topology_splitter import AreaTopology, split_topology, _base_bus


TOPOLOGY_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "topology.json")


@pytest.fixture(scope="module")
def topology() -> Topology:
    with open(TOPOLOGY_PATH) as f:
        return Topology.parse_obj(json.load(f))


@pytest.fixture(scope="module")
def areas(topology) -> list[AreaTopology]:
    return split_topology(topology)


# ---------------------------------------------------------------------------
# Basic structure tests
# ---------------------------------------------------------------------------


class TestSplitReturnsCorrectCount:
    def test_returns_list(self, areas):
        assert isinstance(areas, list)

    def test_returns_five_areas_for_ieee123(self, areas):
        """The IEEE 123-bus feeder has 4 boundary switches → 5 areas."""
        assert len(areas) == 5

    def test_all_items_are_area_topology(self, areas):
        for a in areas:
            assert isinstance(a, AreaTopology)

    def test_area_indices_are_sequential(self, areas):
        indices = [a.area_index for a in areas]
        assert sorted(indices) == list(range(len(areas)))


# ---------------------------------------------------------------------------
# Source / slack bus tests
# ---------------------------------------------------------------------------


class TestSourceBus:
    def test_exactly_one_area_has_original_slack_bus(self, topology, areas):
        """Exactly one area should retain the original slack bus."""
        original_slack_base = _base_bus(topology.slack_bus[0])
        slack_areas = [a for a in areas if a.source_bus == original_slack_base]
        assert len(slack_areas) == 1

    def test_original_slack_area_has_original_slack_phases(self, topology, areas):
        original_slack_base = _base_bus(topology.slack_bus[0])
        source_area = next(a for a in areas if a.source_bus == original_slack_base)
        assert sorted(source_area.topology.slack_bus) == sorted(topology.slack_bus)

    def test_non_source_areas_have_non_empty_slack_bus(self, topology, areas):
        original_slack_base = _base_bus(topology.slack_bus[0])
        for a in areas:
            if a.source_bus != original_slack_base:
                assert (
                    len(a.topology.slack_bus) > 0
                ), f"Area {a.area_index} has empty slack_bus"

    def test_non_source_slack_bus_phases_exist_in_voltages(self, areas, topology):
        original_slack_base = _base_bus(topology.slack_bus[0])
        all_voltage_ids = set(topology.base_voltage_magnitudes.ids)
        for a in areas:
            if a.source_bus == original_slack_base:
                continue
            for sb in a.topology.slack_bus:
                assert (
                    sb in all_voltage_ids
                ), f"Area {a.area_index} slack_bus {sb!r} not in original voltage IDs"


# ---------------------------------------------------------------------------
# Boundary switch tests
# ---------------------------------------------------------------------------


class TestBoundarySwitches:
    def test_source_area_has_boundary_switches(self, topology, areas):
        """The source area (containing original slack) should have ≥1 boundary switch."""
        original_slack_base = _base_bus(topology.slack_bus[0])
        source_area = next(a for a in areas if a.source_bus == original_slack_base)
        assert len(source_area.switch_ids) >= 1

    def test_boundary_switches_appear_in_both_areas(self, areas):
        """Each boundary switch ID must appear in exactly 2 areas."""
        from collections import Counter

        switch_counter: Counter = Counter()
        for a in areas:
            for sid in a.switch_ids:
                switch_counter[sid] += 1

        for sid, count in switch_counter.items():
            assert (
                count == 2
            ), f"Switch {sid!r} appears in {count} areas, expected exactly 2"

    def test_boundary_switches_in_incidences(self, areas):
        """Each area's boundary switches should appear in that area's incidences."""
        for a in areas:
            incidence_ids = set(a.topology.incidences.ids)
            for sid in a.switch_ids:
                assert (
                    sid in incidence_ids
                ), f"Switch {sid!r} missing from area {a.area_index} incidences"


# ---------------------------------------------------------------------------
# Data filtering tests
# ---------------------------------------------------------------------------


class TestDataFiltering:
    def _area_nodes(self, area: AreaTopology) -> set:
        """Get the set of base bus names in an area from its voltage magnitudes."""
        if area.topology.base_voltage_magnitudes is None:
            return set()
        return {_base_bus(nid) for nid in area.topology.base_voltage_magnitudes.ids}

    def test_incidence_buses_belong_to_area(self, areas):
        for a in areas:
            nodes = self._area_nodes(a)
            inc = a.topology.incidences
            for f, t in zip(inc.from_equipment, inc.to_equipment):
                assert (
                    _base_bus(f) in nodes
                ), f"Area {a.area_index}: incidence from_bus {_base_bus(f)!r} not in area nodes"
                assert (
                    _base_bus(t) in nodes
                ), f"Area {a.area_index}: incidence to_bus {_base_bus(t)!r} not in area nodes"

    def test_admittance_buses_belong_to_area(self, areas):
        for a in areas:
            nodes = self._area_nodes(a)
            adm = a.topology.admittance
            for f, t in zip(adm.from_equipment, adm.to_equipment):
                assert (
                    _base_bus(f) in nodes
                ), f"Area {a.area_index}: admittance from {_base_bus(f)!r} not in area nodes"
                assert (
                    _base_bus(t) in nodes
                ), f"Area {a.area_index}: admittance to {_base_bus(t)!r} not in area nodes"

    def test_injection_buses_belong_to_area(self, areas):
        for a in areas:
            nodes = self._area_nodes(a)
            for nid in a.topology.injections.power_real.ids:
                assert (
                    _base_bus(nid) in nodes
                ), f"Area {a.area_index}: injection node {_base_bus(nid)!r} not in area nodes"
            for nid in a.topology.injections.power_imaginary.ids:
                assert (
                    _base_bus(nid) in nodes
                ), f"Area {a.area_index}: injection node {_base_bus(nid)!r} not in area nodes"

    def test_voltage_ids_are_subset_of_originals(self, topology, areas):
        """Each area's voltage IDs must be a subset of the original voltage IDs."""
        original_ids = set(topology.base_voltage_magnitudes.ids)
        for a in areas:
            if a.topology.base_voltage_magnitudes is None:
                continue
            for vid in a.topology.base_voltage_magnitudes.ids:
                assert (
                    vid in original_ids
                ), f"Area {a.area_index}: voltage id {vid!r} not in original topology"

    def test_all_original_voltage_ids_covered(self, topology, areas):
        """Every non-OPEN voltage ID from the original topology appears in at least one area.

        Buses with 'OPEN' in their name are excluded by the graph generator and
        are therefore intentionally absent from the area topologies.
        """
        covered = set()
        for a in areas:
            if a.topology.base_voltage_magnitudes:
                covered.update(a.topology.base_voltage_magnitudes.ids)
        for vid in topology.base_voltage_magnitudes.ids:
            if "OPEN" in vid:
                continue  # graph generator skips OPEN-flagged buses
            assert vid in covered, f"Voltage id {vid!r} not covered by any area"

    def test_injection_values_are_preserved(self, topology, areas):
        """Injection values in each area match those in the original topology.

        The original topology can contain duplicate node IDs with different values;
        we verify each area entry is a valid (id, value) pair from the original.
        """
        original_pairs = set(
            zip(
                topology.injections.power_real.ids,
                topology.injections.power_real.values,
            )
        )
        for a in areas:
            for nid, val in zip(
                a.topology.injections.power_real.ids,
                a.topology.injections.power_real.values,
            ):
                assert (
                    nid,
                    val,
                ) in original_pairs, f"Area {a.area_index}: injection ({nid}, {val}) not in original topology"

    def test_admittance_lists_lengths_match(self, areas):
        """Admittance list length must equal from_equipment length in every area."""
        for a in areas:
            adm = a.topology.admittance
            assert len(adm.admittance_list) == len(adm.from_equipment)
            assert len(adm.admittance_list) == len(adm.to_equipment)

    def test_incidence_lengths_consistent(self, areas):
        """Incidence from/to/ids arrays must all have the same length."""
        for a in areas:
            inc = a.topology.incidences
            assert len(inc.from_equipment) == len(inc.to_equipment)
            assert len(inc.from_equipment) == len(inc.ids)
