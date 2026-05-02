#!/usr/bin/env python3
"""
EXPOSE - Stage 4: Persona Campaigns and Shadow Scam Ecosystems (Paper §3.4, §6)
================================================================================

Implements the three-level hierarchy of Section 6:

    Level 1 (Identifier) : individual phone number
    Level 2 (Campaign)   : connected component of the thresholded
                           complaint-text similarity graph
    Level 3 (Ecosystem)  : connected component of the campaign graph
                           induced by infrastructure indicators

Indicators used to link campaigns into ecosystems are, in decreasing
strength:

    sigma_hub      shared callback target (paper §6.3)
    sigma_edge     direct cross-reference between campaign members
    sigma_carrier  shared dominant carrier (only available with
                   carrier_lookup.jsonl present)

Two ecosystem graphs are built (paper Equations 1 and 2):

    primary    = sigma_hub OR sigma_edge
    augmented  = sigma_hub OR sigma_edge OR sigma_carrier

Inputs
------
    --input PATH         JSONL corpus (default: results.jsonl)
    --output DIR         output directory (default: ./output)
    --carrier PATH       Stage 3 carrier_lookup.jsonl (optional);
                         needed for sigma_carrier
    --threshold FLOAT    cosine-similarity threshold (default: 0.8)

Outputs (under DIR)
-------------------
    campaigns.csv                non-singleton text-similarity campaigns
    ecosystem_links.csv          every campaign-pair link (with indicator)
    ecosystems_primary.csv       ecosystems from primary graph (paper §6.3)
    ecosystems_augmented.csv     ecosystems from augmented graph
    stage4_eval.csv              per-ecosystem analyst-review sheet
    stage4_report.txt            full report
"""
import argparse
import csv
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

DEFAULT_THRESHOLD = 0.8
MIN_TEXT_LENGTH = 50
TFIDF_MAX_FEATURES = 10000
TFIDF_NGRAM_RANGE = (1, 2)
SIM_BATCH_SIZE = 500

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


# ----------------------------------------------------------------------
# Union-find
# ----------------------------------------------------------------------

class UnionFind:
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        # path compression
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return False
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1
        return True

    def groups(self):
        out = defaultdict(set)
        for x in self.parent:
            out[self.find(x)].add(x)
        return dict(out)


# ----------------------------------------------------------------------
# Stage 4(a): Persona campaigns from text similarity
# ----------------------------------------------------------------------

def build_campaigns(records, candidates_set, threshold):
    """Returns list of campaigns and the phone -> campaign id map."""
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    print('\n' + '=' * 60)
    print('STAGE 4(a): persona-based campaigns from complaint text')
    print('=' * 60)

    phone_text = {}
    for r in records:
        bits = [c.get('text', '').strip() for c in r.get('comments', [])]
        bits = [b for b in bits if b]
        if bits:
            phone_text[r['e164']] = ' '.join(bits)[:1000]

    candidates = {
        e: phone_text[e]
        for e in candidates_set
        if e in phone_text and len(phone_text[e]) >= MIN_TEXT_LENGTH
    }
    print(f'  Numbers with >= {MIN_TEXT_LENGTH} chars : {len(candidates):,}')
    if len(candidates) < 10:
        print('  Too few candidates; skipping clustering.')
        return [], {}

    keys = sorted(candidates)
    docs = [candidates[k] for k in keys]
    print(f'  Building TF-IDF (n-gram=1..2, max_features={TFIDF_MAX_FEATURES})')
    vec = TfidfVectorizer(
        max_features=TFIDF_MAX_FEATURES,
        stop_words='english',
        min_df=2,
        max_df=0.5,
        ngram_range=TFIDF_NGRAM_RANGE,
        sublinear_tf=True,
    )
    M = vec.fit_transform(docs)
    print(f'  Matrix: {M.shape[0]} docs x {M.shape[1]} features')

    print('  Pairwise cosine similarity...')
    n = len(keys)
    pairs = []
    for i in range(0, n, SIM_BATCH_SIZE):
        end = min(i + SIM_BATCH_SIZE, n)
        sims = cosine_similarity(M[i:end], M)
        for li in range(end - i):
            gi = i + li
            for j in range(gi + 1, n):
                if sims[li, j] >= threshold:
                    pairs.append((keys[gi], keys[j], float(sims[li, j])))
        if i and i % (SIM_BATCH_SIZE * 5) == 0:
            print(f'    {end}/{n} ({len(pairs)} pairs so far)')
    print(f'  Pairs >= {threshold}: {len(pairs):,}')

    uf = UnionFind()
    for k in keys:
        uf.find(k)
    for a, b, _ in pairs:
        uf.union(a, b)

    campaigns = []
    phone_to_campaign = {}
    for _, members in sorted(uf.groups().items(), key=lambda x: -len(x[1])):
        if len(members) < 2:
            continue
        cid = f'TC_{len(campaigns):04d}'
        campaigns.append({'id': cid, 'members': members, 'size': len(members)})
        for m in members:
            phone_to_campaign[m] = cid

    if campaigns:
        sizes = sorted([c['size'] for c in campaigns], reverse=True)
        print(f'  Non-singleton campaigns : {len(campaigns)}')
        print(f'  Largest = {sizes[0]}, median = {sizes[len(sizes)//2]}')
    return campaigns, phone_to_campaign


# ----------------------------------------------------------------------
# Stage 4(b): Ecosystem linking
# ----------------------------------------------------------------------

def collect_signals(records, R, campaigns, carrier_lookup):
    """Build the per-campaign infrastructure dictionaries."""
    cb_targets = defaultdict(set)
    for r in records:
        for c in r.get('comments', []):
            for mn in c.get('mentioned_numbers', []):
                t = mn.get('e164', '')
                if t and t != r['e164'] and mn.get('context') == 'callback number':
                    cb_targets[t].add(r['e164'])

    phone_to_domains = defaultdict(set)
    for r in records:
        for c in r.get('comments', []):
            for u in URL_PATTERN.findall(c.get('text', '')):
                try:
                    d = (urlparse(u).netloc.lower()
                         .replace('www.', '').strip('.'))
                    if d and d not in SKIP_DOMAINS and len(d) > 3:
                        if not any(s in d for s in SKIP_DOMAINS):
                            phone_to_domains[r['e164']].add(d)
                except Exception:
                    pass

    # sigma_edge: a phone from one campaign appears in another campaign's
    # complaint text.  Accept every platform context (paper §6.3: "a
    # phone number from one campaign appears in the complaint text of
    # another"), but exclude callback edges -- those are already
    # captured by sigma_hub when the target is a callback hub, so
    # double-counting them under sigma_edge inflates the link count
    # without adding signal.
    xref_adj = defaultdict(set)
    for r in records:
        for c in r.get('comments', []):
            for mn in c.get('mentioned_numbers', []):
                t = mn.get('e164', '')
                if t and t != r['e164'] and mn.get('context') != 'callback number':
                    xref_adj[r['e164']].add(t)
                    xref_adj[t].add(r['e164'])

    K_hubs    = defaultdict(set)
    K_domains = defaultdict(set)
    K_carriers = defaultdict(set)
    for c in campaigns:
        for m in c['members']:
            for hub, srcs in cb_targets.items():
                if m in srcs:
                    K_hubs[c['id']].add(hub)
                    if hub in carrier_lookup:
                        cn = carrier_lookup[hub].get('carrier_name')
                        if cn:
                            K_carriers[c['id']].add(cn)
            K_domains[c['id']] |= phone_to_domains.get(m, set())

    return cb_targets, K_hubs, K_domains, K_carriers, xref_adj


def link(campaigns, K_hubs, K_domains, K_carriers, xref_adj):
    """Yield indicator-tagged links between campaign pairs."""
    ids = [c['id'] for c in campaigns]
    by_id = {c['id']: c for c in campaigns}
    links = []

    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            shared = K_hubs[ids[i]] & K_hubs[ids[j]]
            if shared:
                links.append({'camp_a': ids[i], 'camp_b': ids[j],
                              'indicator': 'sigma_hub',
                              'shared': ';'.join(sorted(shared))})

    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            shared = K_domains[ids[i]] & K_domains[ids[j]]
            if shared:
                links.append({'camp_a': ids[i], 'camp_b': ids[j],
                              'indicator': 'shared_domain',
                              'shared': ';'.join(sorted(shared))})

    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            shared = K_carriers[ids[i]] & K_carriers[ids[j]]
            if shared:
                links.append({'camp_a': ids[i], 'camp_b': ids[j],
                              'indicator': 'sigma_carrier',
                              'shared': ';'.join(sorted(shared))})

    for i in range(len(ids)):
        members_i = by_id[ids[i]]['members']
        for j in range(i + 1, len(ids)):
            members_j = by_id[ids[j]]['members']
            connected = any(xref_adj[m] & members_j for m in members_i)
            if connected:
                links.append({'camp_a': ids[i], 'camp_b': ids[j],
                              'indicator': 'sigma_edge',
                              'shared': ''})

    return links


def ecosystems_from_indicators(campaigns, links, indicators):
    uf = UnionFind()
    for c in campaigns:
        uf.find(c['id'])
    for l in links:
        if l['indicator'] in indicators:
            uf.union(l['camp_a'], l['camp_b'])

    by_id = {c['id']: c for c in campaigns}
    ecos = []
    for _, ids in sorted(uf.groups().items(), key=lambda x: -len(x[1])):
        if len(ids) < 2:
            continue
        camps = [by_id[i] for i in ids if i in by_id]
        phones = set().union(*(c['members'] for c in camps))
        ecos.append({
            'id':              f'O_{len(ecos):03d}',
            'campaigns':       sorted(ids),
            'campaign_count':  len(camps),
            'phones':          sorted(phones),
            'phone_count':     len(phones),
        })
    return ecos


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--input', '-i', default='results.jsonl')
    ap.add_argument('--output', '-o', default='output')
    ap.add_argument('--carrier', default=None,
                    help='Stage 3 carrier_lookup.jsonl (enables sigma_carrier)')
    ap.add_argument('--threshold', type=float, default=DEFAULT_THRESHOLD)
    args = ap.parse_args()

    in_path = Path(args.input)
    out_dir = Path(args.output)
    if not in_path.exists():
        print(f'ERROR: input not found: {in_path}', file=sys.stderr)
        sys.exit(1)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Auto-detect carrier file if not specified
    carrier_path = Path(args.carrier) if args.carrier else (out_dir / 'carrier_lookup.jsonl')
    carrier_lookup = {}
    if carrier_path.exists():
        for line in open(carrier_path, 'r', encoding='utf-8'):
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            if r.get('lookup_success'):
                carrier_lookup[r['e164']] = r
        print(f'Loaded carrier lookup : {len(carrier_lookup):,} entries from {carrier_path}')
    else:
        print(f'No carrier lookup at {carrier_path}; sigma_carrier disabled.')

    # Load corpus
    print('Loading corpus...')
    records = []
    with open(in_path, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    R = {r['e164'] for r in records}
    main_lookup = {r['e164']: r for r in records}
    print(f'  Records: {len(records):,}')

    # Numbers that participate in at least one cross-reference
    candidates = set()
    for r in records:
        s = r['e164']
        for c in r.get('comments', []):
            for mn in c.get('mentioned_numbers', []):
                t = mn.get('e164', '')
                if t and t != s:
                    candidates.add(s)
                    if t in R:
                        candidates.add(t)
    print(f'  Cross-reference participants : {len(candidates):,}')

    # Stage 4(a)
    campaigns, phone_to_K = build_campaigns(records, candidates, args.threshold)
    if not campaigns:
        print('No campaigns found; aborting.')
        return

    # Stage 4(b)
    print('\n' + '=' * 60)
    print('STAGE 4(b): linking campaigns into ecosystems')
    print('=' * 60)
    cb_targets, K_hubs, K_domains, K_carriers, xref_adj = \
        collect_signals(records, R, campaigns, carrier_lookup)
    links = link(campaigns, K_hubs, K_domains, K_carriers, xref_adj)
    by_indicator = Counter(l['indicator'] for l in links)
    print('  Cross-campaign links by indicator:')
    for k, v in by_indicator.most_common():
        print(f'    {k:<20} : {v}')
    print(f'  Total: {len(links)}')

    primary = ecosystems_from_indicators(
        campaigns, links, indicators={'sigma_hub', 'sigma_edge'})
    augmented = ecosystems_from_indicators(
        campaigns, links,
        indicators={'sigma_hub', 'sigma_edge', 'sigma_carrier'})
    print(f'\n  Primary    ecosystems (sigma_hub OR sigma_edge)              : {len(primary)}')
    print(f'  Augmented  ecosystems (sigma_hub OR sigma_edge OR sigma_carrier): {len(augmented)}')

    # ------------------------------------------------------------------
    # Persist
    # ------------------------------------------------------------------
    p_camps = out_dir / 'campaigns.csv'
    p_links = out_dir / 'ecosystem_links.csv'
    p_pri   = out_dir / 'ecosystems_primary.csv'
    p_aug   = out_dir / 'ecosystems_augmented.csv'
    p_eval  = out_dir / 'stage4_eval.csv'
    p_rep   = out_dir / 'stage4_report.txt'

    with open(p_camps, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(['campaign_id', 'size', 'phone_numbers',
                    'top_call_type', 'sample_text'])
        for c in campaigns:
            types = Counter(main_lookup[m]['dominant_call_type']
                            for m in c['members'] if m in main_lookup)
            sample = ''
            mem = next(iter(c['members']))
            for cm in main_lookup.get(mem, {}).get('comments', []):
                sample = cm.get('text', '')[:200]
                if sample:
                    break
            w.writerow([c['id'], c['size'], ';'.join(sorted(c['members'])),
                        types.most_common(1)[0][0] if types else '', sample])
    print(f'\nSaved: {p_camps}  ({len(campaigns)} campaigns)')

    with open(p_links, 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['camp_a', 'camp_b',
                                           'indicator', 'shared'])
        w.writeheader()
        w.writerows(links)
    print(f'Saved: {p_links}  ({len(links)} links)')

    def write_ecos(path, ecos):
        with open(path, 'w', encoding='utf-8', newline='') as f:
            w = csv.writer(f)
            w.writerow(['ecosystem_id', 'campaigns', 'phones',
                        'campaign_ids', 'phone_numbers'])
            for e in ecos:
                w.writerow([e['id'], e['campaign_count'], e['phone_count'],
                            ';'.join(e['campaigns']),
                            ';'.join(e['phones'])])
    write_ecos(p_pri, primary)
    write_ecos(p_aug, augmented)
    print(f'Saved: {p_pri}  ({len(primary)} ecosystems)')
    print(f'Saved: {p_aug}  ({len(augmented)} ecosystems)')

    # Eval sheet for analyst review
    by_id = {c['id']: c for c in campaigns}
    with open(p_eval, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(['ecosystem_id', 'graph', 'campaigns', 'phones',
                    'sample_text_1', 'sample_text_2', 'eval_correct'])
        for graph_name, ecos in (('primary', primary), ('augmented', augmented)):
            for e in ecos:
                texts = []
                for cid in e['campaigns'][:2]:
                    c = by_id.get(cid)
                    if c:
                        mem = next(iter(c['members']))
                        for cm in main_lookup.get(mem, {}).get('comments', []):
                            t = cm.get('text', '')[:150]
                            if t:
                                texts.append(f'[{cid}, {c["size"]}ph]: {t}')
                                break
                w.writerow([
                    e['id'], graph_name,
                    e['campaign_count'], e['phone_count'],
                    texts[0] if len(texts) > 0 else '',
                    texts[1] if len(texts) > 1 else '',
                    '',
                ])
    print(f'Saved: {p_eval}')

    lines = []
    lines.append('=' * 70)
    lines.append('EXPOSE - Stage 4: Campaigns and Shadow Scam Ecosystems  (paper §6)')
    lines.append('=' * 70)
    lines.append(f'Date  : {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    lines.append(f'Input : {len(records):,} records')
    lines.append(f'Cross-reference participants : {len(candidates):,}')
    lines.append(f'Carrier lookup entries       : {len(carrier_lookup):,}')
    lines.append('')
    lines.append('LEVEL 2: PERSONA CAMPAIGNS')
    lines.append('-' * 70)
    lines.append(f'  Method     : TF-IDF cosine >= {args.threshold}')
    lines.append(f'  Features   : {TFIDF_MAX_FEATURES} max, n-gram (1,2), sublinear TF')
    lines.append(f'  Campaigns  : {len(campaigns)}')
    if campaigns:
        sizes = sorted([c['size'] for c in campaigns], reverse=True)
        lines.append(f'  Largest    : {sizes[0]} phones')
        lines.append(f'  Median     : {sizes[len(sizes)//2]} phones')
    lines.append('')
    lines.append('LEVEL 3: SHADOW SCAM ECOSYSTEMS')
    lines.append('-' * 70)
    lines.append(f'  Indicator counts:')
    for k, v in by_indicator.most_common():
        lines.append(f'    {k:<20} : {v}')
    lines.append(f'  Total cross-campaign links: {len(links)}')
    lines.append('')
    lines.append(f'  Primary graph   (sigma_hub OR sigma_edge) :')
    lines.append(f'    ecosystems = {len(primary)}, '
                 f'campaigns = {sum(e["campaign_count"] for e in primary)}, '
                 f'phones = {sum(e["phone_count"] for e in primary)}')
    lines.append(f'  Augmented graph (sigma_hub OR sigma_edge OR sigma_carrier) :')
    lines.append(f'    ecosystems = {len(augmented)}, '
                 f'campaigns = {sum(e["campaign_count"] for e in augmented)}, '
                 f'phones = {sum(e["phone_count"] for e in augmented)}')
    lines.append('')
    lines.append('=' * 70)

    with open(p_rep, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'Saved: {p_rep}')
    print()
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
