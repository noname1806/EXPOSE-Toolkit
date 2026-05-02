#!/usr/bin/env python3
"""
EXPOSE - Stage 2: Cross-Reference Graph Construction (Paper §3.2, §4)
======================================================================

Builds the directed cross-reference graph G = (V, E, w) over phone
numbers in the complaint corpus and partitions V into the three roles
introduced in Section 4.2:

    frontline  =  R \\ M     (has complaint page, never mentioned)
    bridge     =  R \\cap M  (has complaint page, also mentioned)
    shadow     =  M \\ R     (mentioned only inside complaint text)

R is the set of directly reported numbers (one complaint page each)
and M is the set of numbers that appear as the target of at least one
complaint-derived observation.  The script also computes a
call-type homogeneity baseline (random + area-code) for the largest
connected components, reproducing the comparison described in §4.

Inputs
------
    --input PATH          JSONL corpus (default: results.jsonl)
    --output DIR          output directory (default: ./output)

Outputs (under DIR)
-------------------
    xref_edges.csv             every directed edge (s, t, context)
    xref_components.csv        every connected component
    xref_component_eval.csv    top-50 components for analyst review
    stage2_baseline_comparison.txt   homogeneity vs. random / area-code
    stage2_report.txt          full report (frontline / bridge / shadow,
                               component sizes, role counts)
"""
import argparse
import csv
import json
import random
import re
import sys
from collections import Counter, defaultdict, deque
from datetime import datetime
from pathlib import Path


# ----------------------------------------------------------------------
# Loading
# ----------------------------------------------------------------------

def load_records(path):
    records = []
    with open(path, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


# ----------------------------------------------------------------------
# Edge extraction
# ----------------------------------------------------------------------

def extract_edges(records):
    """
    Each (source_complaint_page, mentioned_number, context) tuple produced
    by the platform parser becomes one observation in the multiset Omega.
    Self-references are removed.
    """
    edges = []
    total = 0
    self_refs = 0

    for r in records:
        source = r['e164']
        for c in r.get('comments', []):
            for mn in c.get('mentioned_numbers', []):
                tgt = mn.get('e164', '')
                total += 1
                if not tgt:
                    continue
                if tgt == source:
                    self_refs += 1
                    continue
                ctx = mn.get('context', 'mentioned in comment')
                edges.append((source, tgt, ctx))

    unique_pairs = {(s, t) for s, t, _ in edges}
    stats = {
        'total_raw_mentions':  total,
        'self_refs_removed':   self_refs,
        'valid_observations':  len(edges),
        'unique_pairs':        len(unique_pairs),
    }
    return edges, stats


# ----------------------------------------------------------------------
# Graph + components (BFS on the undirected projection)
# ----------------------------------------------------------------------

def build_adjacency(edges):
    adj = defaultdict(set)
    for s, t, _ in edges:
        adj[s].add(t)
        adj[t].add(s)
    return adj


def find_components(adj):
    visited = set()
    comps = []
    for start in adj:
        if start in visited:
            continue
        comp, q = set(), deque([start])
        while q:
            n = q.popleft()
            if n in visited:
                continue
            visited.add(n)
            comp.add(n)
            for nb in adj[n]:
                if nb not in visited:
                    q.append(nb)
        comps.append(comp)
    comps.sort(key=len, reverse=True)
    return comps


# ----------------------------------------------------------------------
# Component analysis
# ----------------------------------------------------------------------

ENTITY_PATTERNS = {
    'SSA':       re.compile(r'social security|ssa\b', re.I),
    'IRS':       re.compile(r'\birs\b|internal revenue', re.I),
    'Amazon':    re.compile(r'\bamazon\b|prime\b', re.I),
    'Apple':     re.compile(r'\bapple\b|icloud|iphone', re.I),
    'Microsoft': re.compile(r'microsoft|windows\s+support', re.I),
    'Loan':      re.compile(r'loan|pre.?approv|debt\s+relief|consolidat', re.I),
    'Police':    re.compile(r'police|arrest|warrant|fbi|dea|marshal', re.I),
    'Prize':     re.compile(r'won|winner|prize|lottery|sweepstakes', re.I),
    'Delivery':  re.compile(r'usps|fedex|ups|package|delivery', re.I),
    'Toll':      re.compile(r'toll|ezpass|sunpass|fastrak', re.I),
    'Warranty':  re.compile(r'warranty|auto\s+warranty', re.I),
    'Medicare':  re.compile(r'medicare|health\s+insurance', re.I),
    'Bank':      re.compile(r'bank\s+of|chase|wells\s+fargo|citibank', re.I),
}


def analyze_components(comps, R, lookup, edges):
    out = []
    for rank, comp in enumerate(comps, 1):
        in_R = comp & R
        in_shadow = comp - R

        types = Counter()
        ents = Counter()
        samples = []
        for n in in_R:
            r = lookup[n]
            types[r['dominant_call_type']] += 1
            for c in r.get('comments', []):
                t = c.get('text', '')
                if t and len(samples) < 3:
                    samples.append((n, t[:250]))
                for ent, pat in ENTITY_PATTERNS.items():
                    if pat.search(t):
                        ents[ent] += 1

        if types:
            top_type, top_count = types.most_common(1)[0]
            h_t = top_count / sum(types.values())
        else:
            top_type, h_t = 'N/A', 0.0
        top_entity = ents.most_common(1)[0][0] if ents else 'N/A'

        cb = sum(1 for s, t, ctx in edges
                 if (s in comp or t in comp) and ctx == 'callback number')

        out.append({
            'rank':          rank,
            'size':          len(comp),
            'corpus_nodes':  len(in_R),
            'shadow_nodes':  len(in_shadow),
            'top_call_type': top_type,
            'homogeneity':   round(h_t, 4),
            'top_entity':    top_entity,
            'callback_edges': cb,
            'sample_texts':  samples,
        })
    return out


def compare_baselines(comp_analysis, R, lookup):
    try:
        from scipy import stats
    except ImportError:
        return None

    xref_h = [c['homogeneity'] for c in comp_analysis if c['corpus_nodes'] >= 2]

    all_types = [lookup[n]['dominant_call_type'] for n in R]
    random.seed(42)
    rand_h = []
    for _ in range(1000):
        sample = random.sample(all_types, min(20, len(all_types)))
        top = Counter(sample).most_common(1)[0][1]
        rand_h.append(top / len(sample))

    ac_groups = defaultdict(list)
    for n in R:
        if n.startswith('+1') and len(n) >= 5:
            ac_groups[n[2:5]].append(lookup[n]['dominant_call_type'])
    ac_h = []
    for _, ts in ac_groups.items():
        if len(ts) >= 5:
            top = Counter(ts).most_common(1)[0][1]
            ac_h.append(top / len(ts))

    t_r, p_r = stats.ttest_ind(xref_h, rand_h[:len(xref_h)])
    t_a, p_a = stats.ttest_ind(xref_h, ac_h[:len(xref_h)])

    return {
        'xref_mean':   sum(xref_h) / len(xref_h) if xref_h else 0,
        'xref_n':      len(xref_h),
        'random_mean': sum(rand_h) / len(rand_h),
        'random_n':    len(rand_h),
        'ac_mean':     sum(ac_h) / len(ac_h) if ac_h else 0,
        'ac_n':        len(ac_h),
        't_vs_random': round(t_r, 3),
        'p_vs_random': round(p_r, 6),
        't_vs_ac':     round(t_a, 3),
        'p_vs_ac':     round(p_a, 6),
    }


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--input', '-i', default='results.jsonl')
    ap.add_argument('--output', '-o', default='output')
    args = ap.parse_args()

    in_path = Path(args.input)
    out_dir = Path(args.output)
    if not in_path.exists():
        print(f'ERROR: input not found: {in_path}', file=sys.stderr)
        sys.exit(1)
    out_dir.mkdir(parents=True, exist_ok=True)

    print('Step 1: Loading data...')
    records = load_records(in_path)
    R = {r['e164'] for r in records}
    lookup = {r['e164']: r for r in records}
    total_comments = sum(len(r.get('comments', [])) for r in records)
    print(f'  Records: {len(records):,}')
    print(f'  Reported numbers (|R|): {len(R):,}')
    print(f'  Total comments: {total_comments:,}')

    print('\nStep 2: Extracting cross-reference observations...')
    edges, edge_stats = extract_edges(records)
    print(f'  Raw mentions      : {edge_stats["total_raw_mentions"]:,}')
    print(f'  Self-refs removed : {edge_stats["self_refs_removed"]:,}')
    print(f'  Observations |Omega|  : {edge_stats["valid_observations"]:,}')
    print(f'  Unique (s, t)     : {edge_stats["unique_pairs"]:,}')
    ctx_counts = Counter(c for _, _, c in edges)
    print('  Context distribution:')
    for ctx, cnt in ctx_counts.most_common():
        print(f'    {ctx}: {cnt:,}')

    print('\nStep 3: Building adjacency on the undirected projection...')
    adj = build_adjacency(edges)
    V_xref = set(adj)
    bridge = V_xref & R
    shadow = V_xref - R
    frontline = R - V_xref
    full_V = V_xref | R
    print(f'  |V| graph-participating : {len(V_xref):,}')
    print(f'  |V| including isolated  : {len(full_V):,}')
    print(f'  Frontline (|R\\M|)       : {len(frontline):,}')
    print(f'  Bridge    (|R cap M|)   : {len(bridge):,}')
    print(f'  Shadow    (|M\\R|)       : {len(shadow):,}')

    print('\nStep 4: Connected components (BFS)...')
    comps = find_components(adj)
    sizes = [len(c) for c in comps]
    print(f'  Components: {len(comps):,}')
    if sizes:
        print(f'  Largest={sizes[0]}  median={sizes[len(sizes)//2]}  smallest={sizes[-1]}')

    print('\nStep 5: Analyzing top-50 components...')
    comp_analysis = analyze_components(comps[:50], R, lookup, edges)

    print('\nStep 6: Baseline comparison (random + area-code)...')
    baselines = compare_baselines(comp_analysis, R, lookup)
    if baselines:
        print(f'  Xref components H_T  : {baselines["xref_mean"]:.3f} (n={baselines["xref_n"]})')
        print(f'  Random baseline H_T  : {baselines["random_mean"]:.3f} (n={baselines["random_n"]})')
        print(f'  Area-code baseline   : {baselines["ac_mean"]:.3f} (n={baselines["ac_n"]})')
        print(f'  vs random : t={baselines["t_vs_random"]} p={baselines["p_vs_random"]}')
        print(f'  vs ac     : t={baselines["t_vs_ac"]} p={baselines["p_vs_ac"]}')
    else:
        print('  scipy not available; skipping t-tests')

    # ------------------------------------------------------------------
    # Persist
    # ------------------------------------------------------------------
    p_edges = out_dir / 'xref_edges.csv'
    p_comps = out_dir / 'xref_components.csv'
    p_eval  = out_dir / 'xref_component_eval.csv'
    p_base  = out_dir / 'stage2_baseline_comparison.txt'
    p_rep   = out_dir / 'stage2_report.txt'

    with open(p_edges, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(['source', 'target', 'context'])
        for s, t, ctx in edges:
            w.writerow([s, t, ctx])
    print(f'\nSaved: {p_edges}  ({len(edges):,} edges)')

    with open(p_comps, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(['rank', 'size', 'corpus_nodes', 'shadow_nodes',
                    'top_call_type', 'homogeneity'])
        for i, comp in enumerate(comps, 1):
            in_R_n = comp & R
            in_S_n = comp - R
            types = Counter(lookup[n]['dominant_call_type']
                            for n in in_R_n if n in lookup)
            if types:
                top, top_c = types.most_common(1)[0]
                h_t = top_c / sum(types.values())
            else:
                top, h_t = 'N/A', 0
            w.writerow([i, len(comp), len(in_R_n), len(in_S_n),
                        top, round(h_t, 4)])
    print(f'Saved: {p_comps}  ({len(comps):,} components)')

    with open(p_eval, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(['rank', 'size', 'corpus_nodes', 'shadow_nodes',
                    'top_call_type', 'homogeneity', 'top_entity',
                    'sample_text_1', 'sample_text_2', 'sample_text_3',
                    'is_real_campaign'])
        for c in comp_analysis:
            t = c['sample_texts']
            t1 = f"[{t[0][0]}]: {t[0][1]}" if len(t) > 0 else ''
            t2 = f"[{t[1][0]}]: {t[1][1]}" if len(t) > 1 else ''
            t3 = f"[{t[2][0]}]: {t[2][1]}" if len(t) > 2 else ''
            w.writerow([c['rank'], c['size'], c['corpus_nodes'], c['shadow_nodes'],
                        c['top_call_type'], c['homogeneity'], c['top_entity'],
                        t1, t2, t3, ''])
    print(f'Saved: {p_eval}  (top-50 for analyst review)')

    with open(p_base, 'w', encoding='utf-8') as f:
        f.write('CROSS-REFERENCE GRAPH HOMOGENEITY vs. BASELINES\n')
        f.write('=' * 60 + '\n\n')
        if baselines:
            f.write(f'Cross-reference comps  H_T = {baselines["xref_mean"]:.3f}'
                    f' (n={baselines["xref_n"]})\n')
            f.write(f'Random grouping        H_T = {baselines["random_mean"]:.3f}'
                    f' (n={baselines["random_n"]})\n')
            f.write(f'Area-code grouping     H_T = {baselines["ac_mean"]:.3f}'
                    f' (n={baselines["ac_n"]})\n\n')
            f.write(f'Xref vs random : t={baselines["t_vs_random"]}'
                    f' p={baselines["p_vs_random"]}\n')
            f.write(f'Xref vs ac     : t={baselines["t_vs_ac"]}'
                    f' p={baselines["p_vs_ac"]}\n')
        else:
            f.write('scipy not installed; t-tests not computed.\n')
    print(f'Saved: {p_base}')

    # Full report
    shadow_obs = sum(1 for s, t, _ in edges if t not in R)
    lines = []
    lines.append('=' * 70)
    lines.append('EXPOSE - Stage 2: Cross-Reference Graph  (paper §3.2, §4)')
    lines.append('=' * 70)
    lines.append(f'Date  : {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    lines.append(f'Input : {len(records):,} records, {total_comments:,} comments')
    lines.append('')
    lines.append('OBSERVATIONS')
    lines.append('-' * 70)
    lines.append(f'  Raw platform mentions     : {edge_stats["total_raw_mentions"]:,}')
    lines.append(f'  Self-references removed   : {edge_stats["self_refs_removed"]:,}')
    lines.append(f'  Valid observations |Omega|: {edge_stats["valid_observations"]:,}')
    lines.append(f'  Unique (source, target)   : {edge_stats["unique_pairs"]:,}')
    lines.append('')
    lines.append('GRAPH')
    lines.append('-' * 70)
    lines.append(f'  Graph-participating nodes : {len(V_xref):,}')
    lines.append(f'  Including isolated R      : {len(full_V):,}')
    lines.append(f'  Observations -> shadow    : '
                 f'{shadow_obs:,} / {len(edges):,} '
                 f'({shadow_obs/len(edges)*100:.1f}%)')
    lines.append('')
    lines.append('ROLE PARTITION  (paper §4.2)')
    lines.append('-' * 70)
    lines.append(f'  Frontline  (R \\ M)        : {len(frontline):,}')
    lines.append(f'  Bridge     (R cap M)      : {len(bridge):,}')
    lines.append(f'  Shadow     (M \\ R)        : {len(shadow):,}')
    lines.append('')
    lines.append('CONNECTED COMPONENTS')
    lines.append('-' * 70)
    lines.append(f'  Total components : {len(comps):,}')
    if sizes:
        lines.append(f'  Largest          : {sizes[0]}')
        lines.append(f'  Median           : {sizes[len(sizes)//2]}')
        for t in [2, 5, 10, 50, 100]:
            cnt = sum(1 for s in sizes if s >= t)
            lines.append(f'  Size >= {t:>3}      : {cnt:,}')
    lines.append('')
    if baselines:
        lines.append('HOMOGENEITY vs. BASELINES')
        lines.append('-' * 70)
        lines.append(f'  Cross-reference comps : {baselines["xref_mean"]:.3f}')
        lines.append(f'  Random baseline       : {baselines["random_mean"]:.3f}')
        lines.append(f'  Area-code baseline    : {baselines["ac_mean"]:.3f}')
        lines.append(f'  vs random : t={baselines["t_vs_random"]}'
                     f' p={baselines["p_vs_random"]}')
        lines.append(f'  vs ac     : t={baselines["t_vs_ac"]}'
                     f' p={baselines["p_vs_ac"]}')
        lines.append('')
    lines.append('=' * 70)

    with open(p_rep, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'Saved: {p_rep}')
    print()
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
