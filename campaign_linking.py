#!/usr/bin/env python3
"""
Cross-Channel Campaign Linking System
======================================
Multi-layer operation discovery from scam complaint data.
Follows the approach of Nahapetyan et al. (IEEE S&P 2024).

This system has two stages:
  Stage 1: Cluster complaints into campaigns by text similarity
           (TF-IDF + cosine similarity, threshold 0.8)
  Stage 2: Link campaigns into operations through four layers
           of shared infrastructure:
           A) Shared callback hubs (SIG edges)
           B) Shared phishing domains (URL extraction)
           C) Shared carrier (Twilio enrichment)
           D) SIG cross-reference edges

Input:
    results_part_1.jsonl, results_part_2.jsonl, results_part_3.jsonl
    twilio_lookup_results.jsonl (optional, for carrier linking)

Output:
    campaign_clusters.csv         - All 203 text-similarity campaigns
    operation_graph.csv           - All cross-campaign links with layer labels
    operations_summary.csv        - Each operation with its campaigns and phones
    campaign_linking_report.txt   - Full human-readable report
    campaign_linking_eval.csv     - Top-50 operations for manual verification
                                   (fill in 'correct' column: yes/no)

Usage:
    python3 campaign_linking.py
    python3 campaign_linking.py results_part_1.jsonl results_part_2.jsonl results_part_3.jsonl

Requirements:
    pip install scikit-learn numpy
"""

import json
import re
import csv
import sys
import os
import numpy as np
from collections import defaultdict, Counter
from datetime import datetime
from urllib.parse import urlparse


# ================================================================
# CONFIGURATION
# ================================================================

TEXT_SIM_THRESHOLD = 0.8    # Cosine similarity threshold for campaigns
MIN_TEXT_LENGTH = 50        # Minimum chars for a complaint to be clustered
TFIDF_MAX_FEATURES = 10000
TFIDF_NGRAM_RANGE = (1, 2)  # Unigrams + bigrams
SIM_BATCH_SIZE = 500        # Batch size for pairwise similarity

# Domains to skip (complaint/reference sites, not scam infrastructure)
SKIP_DOMAINS = {
    '800notes.com', 'whocallsme.com', 'callercomplaints.com', 'spokeo.com',
    'whitepages.com', 'google.com', 'youtube.com', 'facebook.com',
    'reddit.com', 'donotcall.gov', 'ftc.gov', 'fcc.gov', 'bbb.org',
    'twitter.com', 'x.com', 'consumer.ftc.gov', 'ic3.gov',
    'complaints.donotcall.gov', 'ftccomplaintassistant.gov',
    'ssa.gov', 'oig.ssa.gov', 'fbi.gov', 'en.wikipedia.org',
    'esupport.fcc.gov', 'consumercomplaints.fcc.gov',
    'treasury.gov', 'consumerfinance.gov', 'irs.gov', 'apple.com',
    'support.apple.com', 'support.google.com', 'yelp.com', 'canada.ca',
    'bankofamerica.com', 'linkedin.com', 'instagram.com', 'pinterest.com',
    'att.com', 'comcast.com', 'verizon.com', 'tmobile.com', 'sprint.com',
}

URL_PATTERN = re.compile(r'https?://[^\s<>"\']+')


# ================================================================
# UNION-FIND
# ================================================================

class UnionFind:
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return False
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1
        return True

    def groups(self):
        groups = defaultdict(set)
        for x in self.parent:
            groups[self.find(x)].add(x)
        return dict(groups)


# ================================================================
# STAGE 1: TEXT-SIMILARITY CAMPAIGN CLUSTERING
# ================================================================

def build_text_campaigns(records, main_set, sig_participants):
    """
    Cluster phone numbers into campaigns based on complaint
    text similarity using TF-IDF + cosine similarity.
    """
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    print("\n" + "=" * 60)
    print("STAGE 1: TEXT-SIMILARITY CAMPAIGN CLUSTERING")
    print("=" * 60)

    # Build text representation per phone number
    phone_texts = {}
    for r in records:
        texts = []
        for c in r.get('comments', []):
            t = c.get('text', '').strip()
            if t:
                texts.append(t)
        if texts:
            phone_texts[r['e164']] = ' '.join(texts)[:1000]

    # Filter to SIG participants with enough text
    candidates = {
        e: phone_texts[e]
        for e in sig_participants
        if e in phone_texts and len(phone_texts[e]) >= MIN_TEXT_LENGTH
    }

    print(f"  Numbers with complaint text: {len(phone_texts):,}")
    print(f"  SIG participants with >= {MIN_TEXT_LENGTH} chars: {len(candidates):,}")

    if len(candidates) < 10:
        print("  WARNING: Too few candidates for clustering")
        return [], {}

    # TF-IDF vectorization
    candidate_phones = sorted(candidates.keys())
    candidate_texts = [candidates[e] for e in candidate_phones]

    print(f"  Building TF-IDF matrix...")
    vectorizer = TfidfVectorizer(
        max_features=TFIDF_MAX_FEATURES,
        stop_words='english',
        min_df=2,
        max_df=0.5,
        ngram_range=TFIDF_NGRAM_RANGE,
        sublinear_tf=True,
    )
    tfidf_matrix = vectorizer.fit_transform(candidate_texts)
    print(f"  TF-IDF matrix: {tfidf_matrix.shape[0]} docs x {tfidf_matrix.shape[1]} features")

    # Pairwise cosine similarity (batched for memory)
    print(f"  Computing pairwise similarities...")
    sim_pairs = []
    n = len(candidate_phones)

    for i in range(0, n, SIM_BATCH_SIZE):
        batch_end = min(i + SIM_BATCH_SIZE, n)
        batch = tfidf_matrix[i:batch_end]
        sims = cosine_similarity(batch, tfidf_matrix)

        for local_idx in range(batch_end - i):
            global_idx = i + local_idx
            for j in range(global_idx + 1, n):
                if sims[local_idx, j] >= TEXT_SIM_THRESHOLD:
                    sim_pairs.append((
                        candidate_phones[global_idx],
                        candidate_phones[j],
                        float(sims[local_idx, j]),
                    ))

        if i % (SIM_BATCH_SIZE * 5) == 0 and i > 0:
            print(f"    Processed {batch_end}/{n} ({len(sim_pairs)} pairs)")

    print(f"  Pairs with cosine >= {TEXT_SIM_THRESHOLD}: {len(sim_pairs):,}")

    # Similarity distribution
    if sim_pairs:
        vals = [s for _, _, s in sim_pairs]
        for t in [0.8, 0.85, 0.9, 0.95]:
            cnt = sum(1 for v in vals if v >= t)
            print(f"    >= {t}: {cnt:,}")

    # Cluster using Union-Find
    uf = UnionFind()
    for phone in candidate_phones:
        uf.find(phone)
    for a, b, sim in sim_pairs:
        uf.union(a, b)

    # Build campaign list (clusters of size >= 2)
    raw_groups = uf.groups()
    campaigns = []
    phone_to_campaign = {}

    for root, members in sorted(raw_groups.items(), key=lambda x: -len(x[1])):
        if len(members) < 2:
            continue
        cid = f"TC_{len(campaigns):04d}"
        campaigns.append({
            'id': cid,
            'members': members,
            'size': len(members),
        })
        for m in members:
            phone_to_campaign[m] = cid

    print(f"\n  Text-similarity campaigns (>= 2 phones): {len(campaigns)}")
    if campaigns:
        sizes = sorted([c['size'] for c in campaigns], reverse=True)
        print(f"  Size: max={sizes[0]}, median={sizes[len(sizes)//2]}, min={sizes[-1]}")
        for t in [2, 3, 5, 10, 20]:
            cnt = sum(1 for s in sizes if s >= t)
            print(f"    >= {t} phones: {cnt} campaigns")

    return campaigns, phone_to_campaign


# ================================================================
# STAGE 2: MULTI-LAYER OPERATION LINKING
# ================================================================

def build_operation_graph(records, main_set, main_lookup, campaigns, twilio_data):
    """
    Link campaigns into operations through four infrastructure layers.
    """
    print("\n" + "=" * 60)
    print("STAGE 2: MULTI-LAYER OPERATION LINKING")
    print("=" * 60)

    camp_ids = [c['id'] for c in campaigns]
    camp_by_id = {c['id']: c for c in campaigns}

    # ── Build infrastructure indices ──

    # Callback targets
    cb_targets = defaultdict(set)
    for r in records:
        for c in r.get('comments', []):
            for mn in c.get('mentioned_numbers', []):
                tgt = mn.get('e164', '')
                if tgt and tgt != r['e164'] and mn.get('context') == 'callback number':
                    cb_targets[tgt].add(r['e164'])

    # Phone-to-domains
    phone_to_domains = defaultdict(set)
    for r in records:
        for c in r.get('comments', []):
            text = c.get('text', '')
            for u in URL_PATTERN.findall(text):
                try:
                    domain = urlparse(u).netloc.lower().replace('www.', '').strip('.')
                    if domain and domain not in SKIP_DOMAINS and len(domain) > 3:
                        if not any(s in domain for s in SKIP_DOMAINS):
                            phone_to_domains[r['e164']].add(domain)
                except:
                    pass

    # All SIG edges (any type)
    sig_adj = defaultdict(set)
    for r in records:
        for c in r.get('comments', []):
            for mn in c.get('mentioned_numbers', []):
                tgt = mn.get('e164', '')
                if tgt and tgt != r['e164']:
                    sig_adj[r['e164']].add(tgt)
                    sig_adj[tgt].add(r['e164'])

    # ── Campaign-level aggregation ──

    campaign_to_hubs = defaultdict(set)
    campaign_to_domains = defaultdict(set)
    campaign_to_carriers = defaultdict(set)

    for camp in campaigns:
        for m in camp['members']:
            # Hubs
            for hub, sources in cb_targets.items():
                if m in sources:
                    campaign_to_hubs[camp['id']].add(hub)
            # Domains
            campaign_to_domains[camp['id']] |= phone_to_domains.get(m, set())
            # Carriers (from Twilio, for callback targets)
            for hub, sources in cb_targets.items():
                if m in sources and hub in twilio_data:
                    carrier = twilio_data[hub].get('carrier_name')
                    if carrier:
                        campaign_to_carriers[camp['id']].add(carrier)

    # ── Layer-by-layer linking ──

    all_links = []

    # Layer A: Shared callback hubs
    print("\n  Layer A: Shared callback hubs")
    layer_a = 0
    for i in range(len(camp_ids)):
        for j in range(i + 1, len(camp_ids)):
            shared = campaign_to_hubs[camp_ids[i]] & campaign_to_hubs[camp_ids[j]]
            if shared:
                all_links.append({
                    'camp_a': camp_ids[i],
                    'camp_b': camp_ids[j],
                    'layer': 'callback_hub',
                    'shared': ';'.join(sorted(shared)),
                })
                layer_a += 1
    print(f"    Links found: {layer_a}")

    # Layer B: Shared domains
    print("  Layer B: Shared domains")
    layer_b = 0
    for i in range(len(camp_ids)):
        for j in range(i + 1, len(camp_ids)):
            shared = campaign_to_domains[camp_ids[i]] & campaign_to_domains[camp_ids[j]]
            if shared:
                all_links.append({
                    'camp_a': camp_ids[i],
                    'camp_b': camp_ids[j],
                    'layer': 'shared_domain',
                    'shared': ';'.join(sorted(shared)),
                })
                layer_b += 1
    print(f"    Links found: {layer_b}")

    # Layer C: Shared carrier
    print("  Layer C: Shared carrier")
    layer_c = 0
    for i in range(len(camp_ids)):
        for j in range(i + 1, len(camp_ids)):
            shared = campaign_to_carriers[camp_ids[i]] & campaign_to_carriers[camp_ids[j]]
            if shared:
                all_links.append({
                    'camp_a': camp_ids[i],
                    'camp_b': camp_ids[j],
                    'layer': 'shared_carrier',
                    'shared': ';'.join(sorted(shared)),
                })
                layer_c += 1
    print(f"    Links found: {layer_c}")

    # Layer D: SIG cross-reference edges between campaigns
    print("  Layer D: SIG cross-reference edges")
    layer_d = 0
    for i in range(len(camp_ids)):
        for j in range(i + 1, len(camp_ids)):
            members_i = camp_by_id[camp_ids[i]]['members']
            members_j = camp_by_id[camp_ids[j]]['members']
            connected = False
            for m in members_i:
                if sig_adj[m] & members_j:
                    connected = True
                    break
            if connected:
                all_links.append({
                    'camp_a': camp_ids[i],
                    'camp_b': camp_ids[j],
                    'layer': 'sig_edge',
                    'shared': '',
                })
                layer_d += 1
    print(f"    Links found: {layer_d}")

    # ── Build operation graph ──
    print(f"\n  Total cross-campaign links: {len(all_links)}")

    uf = UnionFind()
    for c in campaigns:
        uf.find(c['id'])
    for link in all_links:
        uf.union(link['camp_a'], link['camp_b'])

    op_groups = uf.groups()
    operations = []
    for root, camp_set in sorted(op_groups.items(), key=lambda x: -len(x[1])):
        if len(camp_set) < 2:
            continue
        camps_in_op = [camp_by_id[cid] for cid in camp_set if cid in camp_by_id]
        all_phones = set()
        for c in camps_in_op:
            all_phones |= c['members']

        # Metadata
        types = Counter(
            main_lookup[m]['dominant_call_type']
            for m in all_phones if m in main_lookup
        )
        acs = set(
            m[2:5] for m in all_phones
            if m.startswith('+1') and len(m) >= 5
        )
        op_links = [
            l for l in all_links
            if l['camp_a'] in camp_set and l['camp_b'] in camp_set
        ]
        link_layers = Counter(l['layer'] for l in op_links)

        shadow_hubs = set()
        for m in all_phones:
            for hub, sources in cb_targets.items():
                if m in sources and hub not in main_set:
                    shadow_hubs.add(hub)

        op_domains = set()
        for cid in camp_set:
            op_domains |= campaign_to_domains.get(cid, set())

        # Sample text
        sample_texts = []
        for c in camps_in_op[:3]:
            member = list(c['members'])[0]
            for comment in main_lookup.get(member, {}).get('comments', []):
                txt = comment.get('text', '')[:150]
                if txt:
                    sample_texts.append((c['id'], c['size'], txt))
                    break

        operations.append({
            'op_id': f"OP_{len(operations):03d}",
            'campaign_count': len(camps_in_op),
            'phone_count': len(all_phones),
            'area_codes': len(acs),
            'campaign_ids': sorted(camp_set),
            'phones': sorted(all_phones),
            'top_call_type': types.most_common(1)[0][0] if types else 'N/A',
            'call_types': dict(types),
            'link_layers': dict(link_layers),
            'shadow_hubs': sorted(shadow_hubs),
            'domains': sorted(op_domains),
            'sample_texts': sample_texts,
        })

    singletons = sum(1 for v in op_groups.values() if len(v) == 1)

    print(f"\n  Operations (>= 2 campaigns): {len(operations)}")
    print(f"  Singleton campaigns: {singletons}")

    # ── Single-layer comparison ──
    print(f"\n  SINGLE-LAYER vs MULTI-LAYER:")
    for layer_name in ['callback_hub', 'shared_domain', 'shared_carrier', 'sig_edge']:
        layer_uf = UnionFind()
        for c in campaigns:
            layer_uf.find(c['id'])
        for l in all_links:
            if l['layer'] == layer_name:
                layer_uf.union(l['camp_a'], l['camp_b'])
        single_ops = sum(1 for v in layer_uf.groups().values() if len(v) >= 2)
        print(f"    {layer_name:20s} alone: {single_ops} operations")
    print(f"    {'ALL COMBINED':20s}:       {len(operations)} operations")

    return operations, all_links


# ================================================================
# MAIN
# ================================================================

def main():
    # Determine input files
    if len(sys.argv) > 1:
        jsonl_files = sys.argv[1:]
    else:
        jsonl_files = [
            'results_part_1.jsonl',
            'results_part_2.jsonl',
            'results_part_3.jsonl',
        ]

    for f in jsonl_files:
        if not os.path.exists(f):
            print(f"ERROR: {f} not found")
            sys.exit(1)

    # Load records
    print("Loading data...")
    records = []
    for f in jsonl_files:
        with open(f, 'r', encoding='utf-8') as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))

    main_set = set(r['e164'] for r in records)
    main_lookup = {r['e164']: r for r in records}
    print(f"Loaded {len(records):,} records")

    # Load Twilio data (optional)
    twilio_data = {}
    twilio_file = 'twilio_lookup_results.jsonl'
    if os.path.exists(twilio_file):
        for line in open(twilio_file, 'r', encoding='utf-8'):
            r = json.loads(line.strip())
            if r.get('lookup_success'):
                twilio_data[r['e164']] = r
        print(f"Loaded {len(twilio_data):,} Twilio records")
    else:
        print(f"No Twilio file found ({twilio_file}), skipping carrier layer")

    # Find SIG participants
    sig_participants = set()
    for r in records:
        for c in r.get('comments', []):
            for mn in c.get('mentioned_numbers', []):
                tgt = mn.get('e164', '')
                if tgt and tgt != r['e164']:
                    sig_participants.add(r['e164'])
                    if tgt in main_set:
                        sig_participants.add(tgt)

    print(f"SIG participants (phones with cross-references): {len(sig_participants):,}")

    # ── Stage 1: Campaign clustering ──
    campaigns, phone_to_campaign = build_text_campaigns(
        records, main_set, sig_participants
    )

    if not campaigns:
        print("No campaigns found. Exiting.")
        return

    # ── Stage 2: Operation linking ──
    operations, all_links = build_operation_graph(
        records, main_set, main_lookup, campaigns, twilio_data
    )

    # ================================================================
    # SAVE OUTPUTS
    # ================================================================

    # 1. Campaign clusters
    with open('campaign_clusters.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'campaign_id', 'size', 'phone_numbers',
            'top_call_type', 'sample_text',
        ])
        for c in campaigns:
            types = Counter(
                main_lookup[m]['dominant_call_type']
                for m in c['members'] if m in main_lookup
            )
            sample = ''
            member = list(c['members'])[0]
            for comment in main_lookup.get(member, {}).get('comments', []):
                sample = comment.get('text', '')[:200]
                if sample:
                    break
            writer.writerow([
                c['id'],
                c['size'],
                ';'.join(sorted(c['members'])),
                types.most_common(1)[0][0] if types else '',
                sample,
            ])
    print(f"\nSaved: campaign_clusters.csv ({len(campaigns)} campaigns)")

    # 2. Operation graph (all links)
    with open('operation_graph.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'camp_a', 'camp_b', 'layer', 'shared',
        ])
        writer.writeheader()
        writer.writerows(all_links)
    print(f"Saved: operation_graph.csv ({len(all_links)} links)")

    # 3. Operations summary
    with open('operations_summary.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'operation_id', 'campaign_count', 'phone_count',
            'area_codes', 'top_call_type', 'link_layers',
            'shadow_hubs', 'domains', 'campaign_ids',
        ])
        for op in operations:
            writer.writerow([
                op['op_id'],
                op['campaign_count'],
                op['phone_count'],
                op['area_codes'],
                op['top_call_type'],
                json.dumps(op['link_layers']),
                ';'.join(op['shadow_hubs']),
                ';'.join(op['domains']),
                ';'.join(op['campaign_ids']),
            ])
    print(f"Saved: operations_summary.csv ({len(operations)} operations)")

    # 4. Manual evaluation file
    with open('campaign_linking_eval.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'operation_id', 'campaigns', 'phones', 'top_type',
            'link_layers', 'sample_text_1', 'sample_text_2',
            'correct',  # USER FILLS THIS: yes / no / partial
        ])
        for op in operations:
            texts = op['sample_texts']
            t1 = f"[{texts[0][0]}, {texts[0][1]}ph]: {texts[0][2]}" if len(texts) > 0 else ''
            t2 = f"[{texts[1][0]}, {texts[1][1]}ph]: {texts[1][2]}" if len(texts) > 1 else ''
            writer.writerow([
                op['op_id'],
                op['campaign_count'],
                op['phone_count'],
                op['top_call_type'],
                json.dumps(op['link_layers']),
                t1, t2,
                '',  # USER FILLS THIS
            ])
    print(f"Saved: campaign_linking_eval.csv ({len(operations)} rows to label)")

    # 5. Full report
    report = []
    report.append("=" * 70)
    report.append("CAMPAIGN LINKING SYSTEM REPORT")
    report.append("=" * 70)
    report.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append(f"Input: {len(records):,} records")
    report.append(f"SIG participants: {len(sig_participants):,}")
    report.append(f"Twilio records: {len(twilio_data):,}")
    report.append("")
    report.append("STAGE 1: TEXT-SIMILARITY CAMPAIGNS")
    report.append("-" * 40)
    report.append(f"  Method: TF-IDF + cosine similarity (threshold={TEXT_SIM_THRESHOLD})")
    report.append(f"  Features: {TFIDF_MAX_FEATURES} max, bigrams, sublinear TF")
    report.append(f"  Campaigns found: {len(campaigns)}")
    if campaigns:
        sizes = sorted([c['size'] for c in campaigns], reverse=True)
        report.append(f"  Largest: {sizes[0]} phones")
        report.append(f"  Median: {sizes[len(sizes)//2]} phones")
    report.append("")
    report.append("STAGE 2: MULTI-LAYER LINKING")
    report.append("-" * 40)
    layer_counts = Counter(l['layer'] for l in all_links)
    for layer, cnt in layer_counts.most_common():
        report.append(f"  {layer:20s}: {cnt} links")
    report.append(f"  {'TOTAL':20s}: {len(all_links)} links")
    report.append(f"  Operations found: {len(operations)}")
    report.append("")

    report.append("OPERATIONS (sorted by size)")
    report.append("-" * 40)
    for op in operations:
        report.append(f"\n  {op['op_id']}: {op['campaign_count']} campaigns, "
                       f"{op['phone_count']} phones, {op['area_codes']} area codes")
        report.append(f"    Call type: {op['top_call_type']}")
        report.append(f"    Links: {op['link_layers']}")
        report.append(f"    Shadow hubs: {len(op['shadow_hubs'])}")
        if op['domains']:
            report.append(f"    Domains: {op['domains'][:5]}")
        for cid, size, txt in op['sample_texts'][:2]:
            report.append(f"    [{cid}, {size}ph]: \"{txt[:120]}\"")

    report.append("")
    report.append("=" * 70)
    report.append("NEXT STEPS")
    report.append("=" * 70)
    report.append("1. Open campaign_linking_eval.csv")
    report.append("2. For each operation, read sample_text_1 and sample_text_2")
    report.append("3. In the 'correct' column, write:")
    report.append("   'yes'     = campaigns clearly belong to same operation")
    report.append("   'no'      = campaigns are unrelated (false link)")
    report.append("   'partial' = some campaigns linked correctly, others not")
    report.append("4. Save and report precision = yes_count / total")
    report.append("=" * 70)

    report_text = '\n'.join(report)
    with open('campaign_linking_report.txt', 'w', encoding='utf-8') as f:
        f.write(report_text)
    print(f"Saved: campaign_linking_report.txt")

    print("\n" + report_text)


if __name__ == '__main__':
    main()
