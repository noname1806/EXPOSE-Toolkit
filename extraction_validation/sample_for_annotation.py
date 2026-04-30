import json
import random
import csv

observations = []
with open('results.jsonl', encoding='utf-8') as f:
    for line in f:
        record = json.loads(line)
        for comment in record.get('comments', []):
            for mention in comment.get('mentioned_numbers', []):
                observations.append({
                    'page_phone': record['e164'],
                    'mentioned_phone': mention['e164'],
                    'label': mention['context'],
                    'comment_text': comment['text'],
                    'comment_date': comment.get('date', ''),
                })

by_label = {}
for obs in observations:
    label = obs['label']
    if label not in by_label:
        by_label[label] = []
    by_label[label].append(obs)

random.seed(42)
sample = []
for label, items in by_label.items():
    k = min(40, len(items))
    sample.extend(random.sample(items, k))

random.shuffle(sample)

with open('annotation_sheet.csv', 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow([
        'sample_id',
        'page_phone',
        'mentioned_phone',
        'comment_text',
        'annotator1_label',
        'annotator2_label',
        'predicted_label',
    ])
    for i, s in enumerate(sample):
        w.writerow([
            i + 1,
            s['page_phone'],
            s['mentioned_phone'],
            s['comment_text'],
            '',
            '',
            s['label'],
        ])

print(f"Sampled {len(sample)} observations")
for label, items in by_label.items():
    count = sum(1 for s in sample if s['label'] == label)
    print(f"  {label}: {count} (of {len(items)} total)")
