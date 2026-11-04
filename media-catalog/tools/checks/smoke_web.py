"""Request the main catalogue pages and report any server errors: python smoke_web.py [MODULE]"""
import importlib, sys, time, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
modname = sys.argv[1] if len(sys.argv) > 1 else "web.app"
mod = importlib.import_module(modname)
from starlette.testclient import TestClient
c = TestClient(mod.app, raise_server_exceptions=False)
from db import query
mid = query("select id from media order by id limit 1")[0]["id"]
bid = (query("select id from books where not hidden order by id limit 1") or [{"id": 0}])[0]["id"]
urls = ["/", "/browse", "/browse?page=2&sort=title", f"/detail/{mid}", "/series", "/series?page=2", "/favourites", "/duplicates",
        "/books", "/books?q=dune", "/books?author=Isaac%20Asimov", "/books?award=any&award_result=winner",
        "/books?decade=1960&sort=rating", "/books?status=missing", "/books?review=1", "/books?page=3",
        f"/books/{bid}", "/books/category/author", "/books/category/subject?order=count", "/books/category/decade",
        "/books/category/nope", "/api/books?q=time", f"/api/books/{bid}", f"/api/books/{bid}/tags", "/api/books/scan/status",
        f"/books/cover/{bid}"]
bad = 0
for u in urls:
    t = time.time()
    r = c.get(u)
    dt = time.time() - t
    flag = "" if r.status_code in (200, 404) else "  <-- FAIL"
    if r.status_code >= 500: bad += 1
    print(f"{r.status_code} {dt*1000:7.0f}ms {u}{flag}")
    if r.status_code >= 500:
        print(r.text[-2000:])
print("failures:", bad)
