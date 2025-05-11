#!/usr/bin/env python3
"""
Entry point for MapReduce prototype.
Usage: python main.py --input ./AComp_Passenger_data_no_error.csv --workers 8
"""
import argparse
from pathlib import Path
from job import MapReduceJob
from mapper import FlightMapper
from reducer import FlightReducer

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="CSV dataset path")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--batch", type=int, default=50_000)
    return p.parse_args()

def main():
    args = parse_args()
    csv_path = Path(args.input).resolve()
    job = MapReduceJob(
        mapper_cls=FlightMapper,
        reducer_cls=FlightReducer,
        num_workers=args.workers,
        lines_per_batch=args.batch,
    )
    top = job.execute(csv_path)
    print("=== Top flyer(s) ===")
    for pid, cnt in top:
        print(f"{pid},{cnt}")

if __name__ == "__main__":
    main()
