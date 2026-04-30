#!/usr/bin/env python3
"""
SIG Construction and Component Evaluation
==========================================
Builds the Scam Infrastructure Graph from complaint data,
finds connected components via BFS, computes homogeneity
metrics, compares against baselines, and generates a CSV
for manual evaluation.

Input:
    results.jsonl (or multiple JSONL part files)

Output:
    sig_edges.csv                - All 9,756 directed edges
    sig_components.csv           - All 2,449 components with metadata
    sig_component_eval.csv       - Top 50 components for manual labeling
    sig_baseline_comparison.txt  - H_T comparison with random + area-code baselines
    sig_construction_report.txt  - Full human-readable report

Usage:
    python3 sig_construction.py results.jsonl
    python3 sig_construction.py results_part_1.jsonl results_part_2.jsonl results_part_3.jsonl
"""

import json
import re
import csv
import sys
import os
import random
from collections import defaultdict, Counter, deque
from datetime import datetime


# ================================================================
# STEP 1: LOAD DATA
# ================================================================

def load_records(jsonl_files):
    """Load all records from one or more JSONL files."""
    records = []
    for f in jsonl_files:
        with open(f, 'r', encoding='utf-8') as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


# ================================================================
# STEP 2: EXTRACT EDGES FROM mentioned_numbers
# ================================================================

def extract_edges(records):
    """
    For each record, look at each comment's mentioned_numbers.
    If comment about phone A mentions phone B (and B != A),
    create a directed edge A -> B with the context label.

    Returns:
        edges: list of (source, target, context) tuples
        stats: dict with extraction statistics
    """
    edges = []
    total_mentions = 0
    self_refs = 0

    for r in records:
        source = r['e164']
        for c in r.get('comments', []):
            for mn in c.get('mentioned_numbers', []):
                tgt = mn.get('e164', '')
                total_mentions += 1

                if not tgt:
                    continue

                # Remove self-references (A mentions itself)
                if tgt == source:
                    self_refs += 1
                    continue

                context = mn.get('context', 'mentioned in comment')
                edges.append((source, tgt, context))

    # Unique pairs
    unique_pairs = set((s, t) for s, t, _ in edges)

    stats = {
        'total_raw_mentions': total_mentions,
        'self_refs_removed': self_refs,
        'valid_edges': len(edges),
        'unique_pairs': len(unique_pairs),
    }

    return edges, stats


# ================================================================
# STEP 3: BUILD ADJACENCY LIST (UNDIRECTED)
# ================================================================

def build_adjacency(edges):
    """
    Build an undirected adjacency list from directed edges.
    If A -> B exists, add both A-B and B-A.

    This is because we want BFS to find ALL nodes connected
    by ANY chain of cross-references, regardless of direction.
    """
    adj = defaultdict(set)
    for s, t, _ in edges:
        adj[s].add(t)
        adj[t].add(s)
    return adj


# ================================================================
# STEP 4: BFS TO FIND CONNECTED COMPONENTS
# ================================================================

def find_components(adj):
    """
    Run BFS from each unvisited node to find connected components.

    BFS works like this:
    1. Pick any unvisited node. Put it in a queue.
    2. Take the first node from the queue.
    3. Mark it as visited. Add it to the current component.
    4. Add all its unvisited neighbors to the queue.
    5. Repeat steps 2-4 until the queue is empty.
    6. Everything visited is one component.
    7. Pick another unvisited node and repeat from step 1.

    Returns list of sets, each set is one component.
    Sorted by size (largest first).
    """
    visited = set()
    components = []

    for start_node in adj:
        if start_node in visited:
            continue

        # BFS from this node
        component = set()
        queue = deque([start_node])

        while queue:
            node = queue.popleft()

            if node in visited:
                continue

            visited.add(node)
            component.add(node)

            # Add all unvisited neighbors
            for neighbor in adj[node]:
                if neighbor not in visited:
                    queue.append(neighbor)

        components.append(component)

    # Sort largest first
    components.sort(key=len, reverse=True)
    return components


# ================================================================
# STEP 5: COMPUTE COMPONENT METRICS
# ================================================================

def analyze_components(components, main_set, main_lookup, edges):
    """
    For each component, compute:
    - Size (total, corpus, shadow)
    - Call-type homogeneity H_T
    - Top entity (IRS, loan, Amazon, etc.)
    - Sample complaint texts
    """

    # Entity detection patterns
    entity_patterns = {
        'SSA': re.compile(r'social security|ssa\b', re.I),
        'IRS': re.compile(r'\birs\b|internal revenue', re.I),
        'Amazon': re.compile(r'\bamazon\b|prime\b', re.I),
        'Apple': re.compile(r'\bapple\b|icloud|iphone', re.I),
        'Microsoft': re.compile(r'microsoft|windows\s+support', re.I),
        'Loan': re.compile(r'loan|pre.?approv|debt\s+relief|consolidat', re.I),
        'Police': re.compile(r'police|arrest|warrant|fbi|dea|marshal', re.I),
        'Prize': re.compile(r'won|winner|prize|lottery|sweepstakes', re.I),
        'Delivery': re.compile(r'usps|fedex|ups|package|delivery', re.I),
        'Toll': re.compile(r'toll|ezpass|sunpass|fastrak', re.I),
        'Warranty': re.compile(r'warranty|auto\s+warranty', re.I),
        'Medicare': re.compile(r'medicare|health\s+insurance', re.I),
        'Bank': re.compile(r'bank\s+of|chase|wells\s+fargo|citibank', re.I),
    }

    results = []

    for rank, comp in enumerate(components):
        corpus_in = comp & main_set
        shadow_in = comp - main_set

        # Call-type distribution (only corpus members have call types)
        call_types = Counter()
        entities = Counter()
        sample_texts = []

        for n in corpus_in:
            r = main_lookup[n]
            call_types[r['dominant_call_type']] += 1

            for c in r.get('comments', []):
                text = c.get('text', '')
                if text and len(sample_texts) < 3:
                    sample_texts.append((n, text[:250]))
                for ent_name, pattern in entity_patterns.items():
                    if pattern.search(text):
                        entities[ent_name] += 1

        # Homogeneity H_T
        if call_types:
            top_type, top_count = call_types.most_common(1)[0]
            h_t = top_count / sum(call_types.values())
        else:
            top_type = 'N/A'
            h_t = 0.0

        # Top entity
        if entities:
            top_entity = entities.most_common(1)[0][0]
        else:
            top_entity = 'N/A'

        # Count callback edges in this component
        cb_edges = 0
        for s, t, ctx in edges:
            if (s in comp or t in comp) and ctx == 'callback number':
                cb_edges += 1

        results.append({
            'rank': rank + 1,
            'size': len(comp),
            'corpus_nodes': len(corpus_in),
            'shadow_nodes': len(shadow_in),
            'top_call_type': top_type,
            'homogeneity': round(h_t, 4),
            'top_entity': top_entity,
            'callback_edges': cb_edges,
            'call_type_breakdown': dict(call_types),
            'sample_texts': sample_texts,
        })

    return results


# ================================================================
# STEP 6: BASELINE COMPARISON
# ================================================================

def compare_baselines(comp_analysis, main_set, main_lookup):
    """
    Compare SIG component homogeneity against two baselines:
    1. Random grouping: randomly sample N numbers, compute H_T
    2. Area-code grouping: group by area code, compute H_T

    Uses t-test to check if SIG is significantly better.
    """
    from scipy import stats

    # SIG H_T values (components with >= 2 corpus members)
    sig_h_t = [
        c['homogeneity'] for c in comp_analysis
        if c['corpus_nodes'] >= 2
    ]

    # Baseline 1: Random grouping
    all_types = [main_lookup[n]['dominant_call_type'] for n in main_set]
    random.seed(42)
    random_h_t = []
    for _ in range(1000):
        sample = random.sample(all_types, min(20, len(all_types)))
        top = Counter(sample).most_common(1)[0][1]
        random_h_t.append(top / len(sample))

    # Baseline 2: Area-code grouping
    ac_groups = defaultdict(list)
    for n in main_set:
        if n.startswith('+1') and len(n) >= 5:
            ac_groups[n[2:5]].append(main_lookup[n]['dominant_call_type'])
    ac_h_t = []
    for ac, types in ac_groups.items():
        if len(types) >= 5:
            top = Counter(types).most_common(1)[0][1]
            ac_h_t.append(top / len(types))

    # T-tests
    t_random, p_random = stats.ttest_ind(sig_h_t, random_h_t[:len(sig_h_t)])
    t_ac, p_ac = stats.ttest_ind(sig_h_t, ac_h_t[:len(sig_h_t)])

    return {
        'sig_mean': sum(sig_h_t) / len(sig_h_t) if sig_h_t else 0,
        'sig_n': len(sig_h_t),
        'random_mean': sum(random_h_t) / len(random_h_t),
        'random_n': len(random_h_t),
        't_vs_random': round(t_random, 3),
        'p_vs_random': round(p_random, 6),
        'ac_mean': sum(ac_h_t) / len(ac_h_t) if ac_h_t else 0,
        'ac_n': len(ac_h_t),
        't_vs_ac': round(t_ac, 3),
        'p_vs_ac': round(p_ac, 6),
    }


# ================================================================
# MAIN
# ================================================================

def main():
    # Determine input files
    if len(sys.argv) > 1:
        jsonl_files = sys.argv[1:]
    else:
        jsonl_files = ['results.jsonl']

    for f in jsonl_files:
        if not os.path.exists(f):
            print(f"ERROR: {f} not found")
            sys.exit(1)

    # ── Step 1: Load ──
    print("Step 1: Loading data...")
    records = load_records(jsonl_files)
    main_set = set(r['e164'] for r in records)
    main_lookup = {r['e164']: r for r in records}
    print(f"  Records: {len(records):,}")
    print(f"  Unique numbers: {len(main_set):,}")

    total_comments = sum(len(r.get('comments', [])) for r in records)
    print(f"  Total comments: {total_comments:,}")

    # ── Step 2: Extract edges ──
    print("\nStep 2: Extracting edges from mentioned_numbers...")
    edges, edge_stats = extract_edges(records)
    print(f"  Raw mentions: {edge_stats['total_raw_mentions']:,}")
    print(f"  Self-refs removed: {edge_stats['self_refs_removed']:,}")
    print(f"  Valid edges: {edge_stats['valid_edges']:,}")
    print(f"  Unique pairs: {edge_stats['unique_pairs']:,}")

    ctx_counts = Counter(c for _, _, c in edges)
    print(f"  Edge types:")
    for ctx, cnt in ctx_counts.most_common():
        print(f"    {ctx}: {cnt:,}")

    # ── Step 3: Build graph ──
    print("\nStep 3: Building adjacency list...")
    adj = build_adjacency(edges)
    all_nodes = set(adj.keys())
    corpus_nodes = all_nodes & main_set
    shadow_nodes = all_nodes - main_set
    print(f"  Nodes in graph: {len(all_nodes):,}")
    print(f"    Corpus: {len(corpus_nodes):,}")
    print(f"    Shadow: {len(shadow_nodes):,}")

    # Full graph including isolated corpus nodes
    total_graph_nodes = len(all_nodes) + (len(main_set) - len(corpus_nodes))
    print(f"  Total graph nodes (incl. isolated): {total_graph_nodes:,}")

    # ── Step 4: BFS ──
    print("\nStep 4: Finding connected components via BFS...")
    components = find_components(adj)
    print(f"  Components found: {len(components):,}")
    sizes = [len(c) for c in components]
    print(f"  Largest: {sizes[0]}, Median: {sizes[len(sizes)//2]}, Smallest: {sizes[-1]}")

    # ── Step 5: Analyze components ──
    print("\nStep 5: Analyzing top 50 components...")
    comp_analysis = analyze_components(
        components[:50], main_set, main_lookup, edges
    )

    # ── Step 6: Baselines ──
    print("\nStep 6: Comparing against baselines...")
    try:
        baselines = compare_baselines(comp_analysis, main_set, main_lookup)
        print(f"  SIG mean H_T:         {baselines['sig_mean']:.3f} (n={baselines['sig_n']})")
        print(f"  Random baseline:      {baselines['random_mean']:.3f} (n={baselines['random_n']})")
        print(f"  Area-code baseline:   {baselines['ac_mean']:.3f} (n={baselines['ac_n']})")
        print(f"  SIG vs Random:  t={baselines['t_vs_random']}, p={baselines['p_vs_random']}")
        print(f"  SIG vs AC:      t={baselines['t_vs_ac']}, p={baselines['p_vs_ac']}")
    except ImportError:
        print("  WARNING: scipy not installed, skipping t-tests")
        print("  Install with: pip install scipy")
        baselines = None

    # ================================================================
    # SAVE OUTPUTS
    # ================================================================

    # 1. All edges
    with open('sig_edges.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['source', 'target', 'context'])
        for s, t, ctx in edges:
            writer.writerow([s, t, ctx])
    print(f"\nSaved: sig_edges.csv ({len(edges):,} edges)")

    # 2. All components (summary)
    with open('sig_components.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'rank', 'size', 'corpus_nodes', 'shadow_nodes',
            'top_call_type', 'homogeneity',
        ])
        for i, comp in enumerate(components):
            corpus_in = comp & main_set
            shadow_in = comp - main_set
            types = Counter(
                main_lookup[n]['dominant_call_type']
                for n in corpus_in if n in main_lookup
            )
            if types:
                top_type = types.most_common(1)[0][0]
                h_t = types.most_common(1)[0][1] / sum(types.values())
            else:
                top_type = 'N/A'
                h_t = 0
            writer.writerow([
                i + 1, len(comp), len(corpus_in), len(shadow_in),
                top_type, round(h_t, 4),
            ])
    print(f"Saved: sig_components.csv ({len(components):,} components)")

    # 3. Top-50 evaluation CSV (for manual labeling)
    with open('sig_component_eval.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'rank', 'size', 'corpus_nodes', 'shadow_nodes',
            'top_call_type', 'homogeneity', 'top_entity',
            'sample_text_1', 'sample_text_2', 'sample_text_3',
            'is_real_campaign',  # USER FILLS: yes / no / unclear
        ])
        for c in comp_analysis:
            texts = c['sample_texts']
            t1 = f"[{texts[0][0]}]: {texts[0][1]}" if len(texts) > 0 else ''
            t2 = f"[{texts[1][0]}]: {texts[1][1]}" if len(texts) > 1 else ''
            t3 = f"[{texts[2][0]}]: {texts[2][1]}" if len(texts) > 2 else ''
            writer.writerow([
                c['rank'], c['size'], c['corpus_nodes'], c['shadow_nodes'],
                c['top_call_type'], c['homogeneity'], c['top_entity'],
                t1, t2, t3,
                '',  # User fills this
            ])
    print(f"Saved: sig_component_eval.csv (50 rows for manual labeling)")

    # 4. Baseline comparison
    with open('sig_baseline_comparison.txt', 'w') as f:
        f.write("SIG COMPONENT HOMOGENEITY vs BASELINES\n")
        f.write("=" * 50 + "\n\n")
        if baselines:
            f.write(f"SIG components (top 50):  mean H_T = {baselines['sig_mean']:.3f} (n={baselines['sig_n']})\n")
            f.write(f"Random grouping:         mean H_T = {baselines['random_mean']:.3f} (n={baselines['random_n']})\n")
            f.write(f"Area-code grouping:      mean H_T = {baselines['ac_mean']:.3f} (n={baselines['ac_n']})\n\n")
            f.write(f"SIG vs Random:  t={baselines['t_vs_random']}, p={baselines['p_vs_random']}\n")
            f.write(f"SIG vs AC:      t={baselines['t_vs_ac']}, p={baselines['p_vs_ac']}\n")
        else:
            f.write("scipy not installed, t-tests not computed\n")
    print(f"Saved: sig_baseline_comparison.txt")

    # 5. Full report
    report = []
    report.append("=" * 70)
    report.append("SIG CONSTRUCTION REPORT")
    report.append("=" * 70)
    report.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append(f"Input: {len(records):,} records, {total_comments:,} comments")
    report.append("")

    report.append("EDGE EXTRACTION")
    report.append("-" * 40)
    report.append(f"  Raw mentions in comments: {edge_stats['total_raw_mentions']:,}")
    report.append(f"  Self-references removed:  {edge_stats['self_refs_removed']:,}")
    report.append(f"  Valid directed edges:     {edge_stats['valid_edges']:,}")
    report.append(f"  Unique (source, target):  {edge_stats['unique_pairs']:,}")
    report.append("")

    report.append("GRAPH")
    report.append("-" * 40)
    report.append(f"  Nodes: {len(all_nodes):,}")
    report.append(f"    Corpus (reported):      {len(corpus_nodes):,}")
    report.append(f"    Shadow (from text only): {len(shadow_nodes):,}")
    report.append("")

    # Edge distribution
    shadow_edges = sum(1 for s, t, _ in edges if t not in main_set)
    report.append(f"  Edges pointing to shadow: {shadow_edges:,} / {len(edges):,} ({shadow_edges/len(edges)*100:.1f}%)")
    report.append("")

    report.append("CONNECTED COMPONENTS")
    report.append("-" * 40)
    report.append(f"  Total components:  {len(components):,}")
    report.append(f"  Largest:           {sizes[0]} nodes")
    report.append(f"  Median:            {sizes[len(sizes)//2]} nodes")
    for t in [2, 5, 10, 50, 100]:
        cnt = sum(1 for s in sizes if s >= t)
        report.append(f"  Size >= {t:>3}: {cnt:,} components")
    report.append("")

    report.append("THREE-LAYER ARCHITECTURE")
    report.append("-" * 40)
    # Frontline: corpus nodes NOT mentioned by others
    mentioned_targets = set(t for _, t, _ in edges)
    frontline = main_set - mentioned_targets
    bridge = main_set & mentioned_targets
    shadow = mentioned_targets - main_set
    report.append(f"  Frontline (corpus, not mentioned): {len(frontline):,}")
    report.append(f"  Bridge (corpus AND mentioned):     {len(bridge):,}")
    report.append(f"  Shadow (mentioned, not corpus):    {len(shadow):,}")
    report.append("")

    if baselines:
        report.append("BASELINE COMPARISON")
        report.append("-" * 40)
        report.append(f"  SIG mean H_T:       {baselines['sig_mean']:.3f}")
        report.append(f"  Random baseline:    {baselines['random_mean']:.3f}")
        report.append(f"  Area-code baseline: {baselines['ac_mean']:.3f}")
        report.append(f"  SIG vs Random: t={baselines['t_vs_random']}, p={baselines['p_vs_random']}")
        report.append(f"  SIG vs AC:     t={baselines['t_vs_ac']}, p={baselines['p_vs_ac']}")
        report.append("")

    report.append("=" * 70)
    report.append("NEXT STEPS")
    report.append("=" * 70)
    report.append("1. Open sig_component_eval.csv")
    report.append("2. For each of the 50 rows, read sample_text_1/2/3")
    report.append("3. In 'is_real_campaign' column, write: yes / no / unclear")
    report.append("4. Report precision = yes_count / (yes_count + no_count)")
    report.append("=" * 70)

    report_text = '\n'.join(report)
    with open('sig_construction_report.txt', 'w') as f:
        f.write(report_text)
    print(f"Saved: sig_construction_report.txt")

    print("\n" + report_text)


if __name__ == '__main__':
    main()
