"""
Reduce phase: aggregate counts per passenger_id.
"""
from collections import defaultdict
from typing import Dict, List, Tuple

class FlightReducer:
    def reduce(self, mapped: List[Tuple[str, int]]) -> Dict[str, int]:
        totals = defaultdict(int)
        for pid, cnt in mapped:
            totals[pid] += cnt
        return totals

    @staticmethod
    def get_top_flyers(totals: Dict[str, int]) -> List[Tuple[str, int]]:
        if not totals:
            return []
        max_cnt = max(totals.values())
        return sorted([(pid, c) for pid, c in totals.items() if c == max_cnt])
