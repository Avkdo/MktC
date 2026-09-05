/**
 * Fetches weighted_shares_outstanding for every active US common-stock ticker
 * and pushes the finished map to the Worker's /admin/shares-outstanding route
 * in chunks, so it lands in KV using the exact same chunk-per-key pattern
 * warmAllChunks() already uses for bars (see worker.js comments on why: KV's
 * 1,000 writes/day limit means writes must scale with CHUNK count, not
 * ticker count).
 *
 * Why this has to be per-ticker at all: Polygon's bulk ticker list endpoint
 * (/v3/reference/tickers) only returns lightweight summaries — ticker, name,
 * market, type, active. Shares outstanding only exists on the single-ticker
 * detail endpoint (/v3/reference/tickers/{ticker}), one round-trip each.
 * There is no bulk shortcut for this field — confirmed against Polygon's
 * docs before building this, not an assumption.
 *
 * Runs as a GitHub Actions job specifically because this loop is thousands
 * of calls — far past what a Cloudflare Worker's scheduled() execution
 * ceiling could reliably finish in one invocation.
 */

const POLYGON_API_KEY = process.env.POLYGON_API_KEY;
const WORKER_URL = process.env.WORKER_URL; // e.g. https://mcv1.alex-vivero.workers.dev
const WORKER_ADMIN_SECRET = process.env.WORKER_ADMIN_SECRET;

if (!POLYGON_API_KEY || !WORKER_URL || !WORKER_ADMIN_SECRET) {
  console.error('Missing required env vars: POLYGON_API_KEY, WORKER_URL, WORKER_ADMIN_SECRET');
  process.exit(1);
}

const CONCURRENCY = 10;          // parallel in-flight detail calls
const CHUNK_SIZE = 400;          // matches worker.js's CHUNK_SIZE for bars — same 25MB/key ceiling logic applies
const PAGE_LIMIT = 1000;         // Polygon's max page size for the list endpoint

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/**
 * Paginates through Polygon's ticker list endpoint to get every active US
 * common-stock ticker symbol. This part IS bulk/cheap — it's only the
 * per-ticker detail call after this that's expensive.
 */
async function getAllActiveTickers() {
  const tickers = [];
  let url = `https://api.polygon.io/v3/reference/tickers?market=stocks&type=CS&active=true&limit=${PAGE_LIMIT}&apiKey=${POLYGON_API_KEY}`;
  let page = 0;
  while (url) {
    page++;
    const res = await fetch(url);
    const data = await res.json();
    if (!res.ok) throw new Error(`Ticker list page ${page} failed: HTTP ${res.status} ${data.error || ''}`);
    (data.results || []).forEach(t => { if (t.ticker) tickers.push(t.ticker); });
    console.log(`Page ${page}: +${data.results?.length || 0} tickers (running total ${tickers.length})`);
    // Polygon's next_url already has the cursor but not the API key
    url = data.next_url ? `${data.next_url}&apiKey=${POLYGON_API_KEY}` : null;
  }
  return tickers;
}

/**
 * Fetches weighted_shares_outstanding for one ticker. Returns null (not a
 * throw) on a miss/error — a handful of delisted/edge-case tickers failing
 * shouldn't abort the whole run, they just get skipped.
 */
async function fetchSharesOutstanding(ticker) {
  const url = `https://api.polygon.io/v3/reference/tickers/${encodeURIComponent(ticker)}?apiKey=${POLYGON_API_KEY}`;
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    const data = await res.json();
    const shares = data?.results?.weighted_shares_outstanding;
    return (typeof shares === 'number' && shares > 0) ? shares : null;
  } catch (err) {
    return null;
  }
}

/**
 * Runs the per-ticker detail fetch across the whole list with bounded
 * concurrency (not all 8,000+ at once — polite pacing, avoids tripping any
 * rate limit regardless of plan).
 */
async function fetchAllSharesOutstanding(tickers) {
  const result = {};
  let idx = 0;
  let done = 0;
  const startedAt = Date.now();

  async function worker() {
    while (idx < tickers.length) {
      const ticker = tickers[idx++];
      const shares = await fetchSharesOutstanding(ticker);
      if (shares != null) result[ticker] = shares;
      done++;
      if (done % 500 === 0) {
        const elapsedMin = ((Date.now() - startedAt) / 60000).toFixed(1);
        console.log(`${done}/${tickers.length} tickers processed (${elapsedMin} min elapsed)`);
      }
    }
  }

  await Promise.all(Array.from({ length: CONCURRENCY }, worker));
  return result;
}

/**
 * Pushes the finished map to the Worker in chunks, matching the
 * bars-chunk:day:N key layout exactly so the reading side (a new
 * /shares-outstanding GET route) can just merge them the same way
 * handleBarsRequest already merges bar chunks.
 */
async function pushToWorker(sharesMap) {
  const tickers = Object.keys(sharesMap);
  const chunks = [];
  for (let i = 0; i < tickers.length; i += CHUNK_SIZE) {
    chunks.push(tickers.slice(i, i + CHUNK_SIZE));
  }

  console.log(`Pushing ${tickers.length} tickers across ${chunks.length} chunks...`);

  for (let i = 0; i < chunks.length; i++) {
    const chunkTickers = chunks[i];
    const payload = {};
    chunkTickers.forEach(t => { payload[t] = sharesMap[t]; });

    const res = await fetch(`${WORKER_URL}/admin/shares-outstanding`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Admin-Secret': WORKER_ADMIN_SECRET,
      },
      body: JSON.stringify({ chunkIndex: i, totalChunks: chunks.length, tickers: payload }),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Chunk ${i} push failed: HTTP ${res.status} ${text}`);
    }
    console.log(`Chunk ${i + 1}/${chunks.length} pushed OK (${chunkTickers.length} tickers)`);
    await sleep(200); // small gap between chunk writes, no real need to rush this
  }
}

async function main() {
  console.log('Fetching full active US ticker list...');
  const tickers = await getAllActiveTickers();
  console.log(`Got ${tickers.length} active tickers. Fetching shares outstanding for each (this is the slow part)...`);

  const sharesMap = await fetchAllSharesOutstanding(tickers);
  const foundCount = Object.keys(sharesMap).length;
  console.log(`Resolved shares outstanding for ${foundCount}/${tickers.length} tickers.`);

  if (foundCount === 0) {
    throw new Error('Got zero results — aborting push so we do not overwrite good data in KV with an empty set.');
  }

  await pushToWorker(sharesMap);
  console.log('Done.');
}

main().catch(err => {
  console.error('FAILED:', err.message);
  process.exit(1);
});
