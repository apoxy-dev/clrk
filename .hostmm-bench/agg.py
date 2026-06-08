#!/usr/bin/env python3
# agg.py — group a CSV (stdin, with header) by key columns and summarize a value column.
# Usage: agg.py <value_col> <key_col1> [key_col2 ...]
# Emits one row per distinct key tuple: keys..., n, min, p50, mean, p95, max
import sys, csv, statistics as st

def pct(xs, q):
    if not xs: return float('nan')
    xs = sorted(xs)
    if len(xs) == 1: return xs[0]
    i = max(0, min(len(xs)-1, int(round(q*(len(xs)-1)))))
    return xs[i]

val = sys.argv[1]
keys = sys.argv[2:]
rows = list(csv.DictReader(sys.stdin))
groups = {}
for r in rows:
    k = tuple(r[c] for c in keys)
    try:
        v = float(r[val])
    except (ValueError, KeyError):
        continue
    groups.setdefault(k, []).append(v)

def sortkey(item):
    out = []
    for x in item[0]:
        try: out.append((0, float(x)))
        except ValueError: out.append((1, x))
    return out

print(",".join(keys) + ",n,min,p50,mean,p95,max")
for k, xs in sorted(groups.items(), key=sortkey):
    print(",".join(k) + ",%d,%.3f,%.3f,%.3f,%.3f,%.3f" % (
        len(xs), min(xs), st.median(xs), st.mean(xs), pct(xs, 0.95), max(xs)))
