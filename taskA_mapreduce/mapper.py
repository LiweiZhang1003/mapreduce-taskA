# -*- coding: utf-8 -*-
"""Mapper module for MapReduce Task A."""
"""
Map phase: each CSV line → (passenger_id, 1)
"""
from typing import List, Tuple

class FlightMapper:
    def map(self, lines: List[str]) -> List[Tuple[str, int]]:
        kv_pairs = []
        for ln in lines:
            ln = ln.strip()
            if not ln:
                continue
            passenger_id = ln.split(",")[0]
            kv_pairs.append((passenger_id, 1))
        return kv_pairs
