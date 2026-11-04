"""Evaluate the Summary sheet's COUNTIF/SUMIF formulas in Python (no LibreOffice here): python check_formulas.py XLSX"""
import fnmatch, re, sys
from openpyxl import load_workbook
from openpyxl.utils import column_index_from_string

wb = load_workbook(sys.argv[1], read_only=True)
cols = {}
def col(sheet, letter, r1, r2):
    key = (sheet, letter)
    if key not in cols:
        i = column_index_from_string(letter) - 1
        cols[key] = [row[i] for row in wb[sheet].iter_rows(min_row=1, values_only=True)]
    return cols[key][r1 - 1:r2]
def rng(ref):
    m = re.fullmatch(r"(?:'?([^'!]+)'?!)?\$?([A-Z]+)\$?(\d+):\$?([A-Z]+)\$?(\d+)", ref)
    return col(m.group(1), m.group(2), int(m.group(3)), int(m.group(5)))
def match(v, crit):
    return fnmatch.fnmatchcase(str(v or ""), crit) if "*" in crit else v == crit
summary = list(wb["Summary"].iter_rows(values_only=True))
vals = {}
def ev(f, row):
    f = f.lstrip("=")
    f = re.sub(r"COUNTIFS\(([^,]+),\"([^\"]*)\",([^,]+),\"([^\"]*)\"\)",
               lambda m: str(sum(match(a, m.group(2)) and match(b, m.group(4)) for a, b in zip(rng(m.group(1)), rng(m.group(3))))), f)
    f = re.sub(r"COUNTIF\(([^,]+),\"([^\"]*)\"\)", lambda m: str(sum(match(v, m.group(2)) for v in rng(m.group(1)))), f)
    f = re.sub(r"SUMIF\(([^,]+),\"([^\"]*)\",([^)]+)\)",
               lambda m: str(sum((s or 0) for v, s in zip(rng(m.group(1)), rng(m.group(3))) if match(v, m.group(2)))), f)
    f = re.sub(r"COUNTA\(([^)]+)\)", lambda m: str(sum(v not in (None, "") for v in rng(m.group(1)))), f)
    f = re.sub(r"SUM\(([A-Z]+)(\d+):([A-Z]+)(\d+)\)",
               lambda m: str(sum(vals[(m.group(1), r)] for r in range(int(m.group(2)), int(m.group(4)) + 1))), f)
    f = re.sub(r"SUM\(([^)]+)\)", lambda m: str(sum((v or 0) for v in rng(m.group(1)))), f)
    return eval(f)
for r, line in enumerate(summary, 1):
    out = []
    for c, v in enumerate(line):
        letter = "ABCDEFG"[c]
        if isinstance(v, str) and v.startswith("="):
            vals[(letter, r)] = ev(v, r)
            out.append(f"{vals[(letter, r)]:,.1f}" if isinstance(vals[(letter, r)], float) else f"{vals[(letter, r)]:,}")
        elif v is not None:
            vals[(letter, r)] = v
            out.append(str(v)[:70])
    if out:
        print(f"{r:3} " + " | ".join(out))
