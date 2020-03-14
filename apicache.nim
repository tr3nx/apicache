import os, httpClient, db_postgres, strutils, locks

type
  CacheEntry = object
    storedname: string
    apiurl: string

var
  db {.threadvar.}: DbConn
  thr: seq[Thread[tuple[c: CacheEntry, cachepath: string]]]
  L: Lock

proc fetchCaches(ct: int): seq[CacheEntry] =
  for row in db.rows(sql"SELECT storedname, apiurl FROM caches WHERE cachetime = ? ORDER BY id DESC", ct):
    result.add CacheEntry(storedname: row[0], apiurl: row[1])

proc cacheApi(d: tuple[c: CacheEntry, cachepath: string]) {.thread.} =
  var client = newHttpClient(timeout = 10000, maxRedirects = 0)
  client.headers = newHttpHeaders({ "Content-Type": "application/json" })

  var filename = d.cachepath & d.c.storedname & ".json"
  var f: File
  if f.open(filename, fmWrite):
    try: f.write(client.getContent(d.c.apiurl))
    finally: f.close

# main
db = open(getEnv("DB_HOST", "localhost"), getEnv("DB_USER", "postgres"), getEnv("DB_PASS", ""), getEnv("DB_DB", "apicache"))

var args = commandLineParams()

initLock(L)

var caches: seq[CacheEntry] = fetchCaches(parseInt(args[1]))
for (i, c) in caches.pairs:
  thr.add Thread[tuple[c: CacheEntry, cachepath: string]]()
  createThread(thr[i], cacheApi, (c, args[0]))

joinThreads(thr)
