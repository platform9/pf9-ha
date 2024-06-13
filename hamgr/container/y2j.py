import yaml
import json
import sys

fname = sys.argv[1]
with open(fname, 'r') as f:
    data = yaml.safe_load(f)
    out = json.dumps(data, separators=(',', ':'))
    print(out)
