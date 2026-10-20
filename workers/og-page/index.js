/**
 * Cloudflare Worker: vasanerd.se/p/{idpe}
 *
 * Fetches the SPA index.html and injects personalized OG meta tags.
 * Crawlers and browsers both get the full page with correct metadata.
 */

const SITE_URL = 'https://vasanerd.se';
const IDPE_RE = /^[a-zA-Z0-9]{8,32}$/;
const RACE_LABELS = {
  vasaloppet: 'Vasaloppet',
  vasaloppet_45: 'Vasaloppet 45',
  vasaloppet_30: 'Vasaloppet 30',
  tjejvasan: 'Tjejvasan',
  ultravasan: 'Ultravasan 90',
  ultravasan_45: 'Ultravasan 45',
  oppet_spar_mandag: 'Öppet Spår måndag 90',
  oppet_spar_sondag: 'Öppet Spår söndag 90',
  cykelvasan_oppet_spar: 'Cykelvasan Öppet Spår',
  birken: 'Birkebeinerrennet',
  nsl: 'Nordenskiöldsloppet',
  lofsdalen_epic: 'Lofsdalen Epic',
  engelbrektsloppet: 'Engelbrektsloppet',
};

/**
 * Resolve idpe -> {name, raceKey, raceLabel, year} via the tiny per-bucket
 * lookup index (site/data/p_index/{xx}.json, ~60 KB) instead of downloading
 * every race's persons.json (~22 MB) on each request. Edge-cached for a day.
 */
async function lookupPerson(idpe) {
  const bucket = idpe.slice(-2).toLowerCase();
  try {
    const resp = await fetch(`${SITE_URL}/data/p_index/${bucket}.json`, {
      cf: { cacheTtl: 86400, cacheEverything: true },
    });
    if (!resp.ok) return null;
    const index = await resp.json();
    const hit = index[idpe];
    if (!hit) return null;
    return {
      name: hit.n || '',
      raceKey: hit.k || '',
      raceLabel: RACE_LABELS[hit.k] || '',
      year: hit.y ? String(hit.y) : '',
    };
  } catch (e) {
    console.error('lookupPerson', idpe, e);
    return null;
  }
}

function esc(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function formatName(raw) {
  const clean = raw.replace(/\s*\(\w+\)/, '');
  const parts = clean.split(/,\s*/);
  return parts.length === 2 ? parts[1] + ' ' + parts[0] : clean;
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/p\/([a-zA-Z0-9]{8,32})$/);
    if (!match) {
      return new Response('Not found', { status: 404 });
    }
    const idpe = match[1];

    // Fetch person metadata + SPA HTML in parallel
    const [meta, spaResp] = await Promise.all([
      getPersonMeta(idpe),
      fetch(`${SITE_URL}/index.html`, { cf: { cacheTtl: 3600, cacheEverything: true } }),
    ]);

    if (!spaResp.ok) {
      return new Response('Failed to load page', { status: 502 });
    }

    let html = await spaResp.text();

    const displayName = meta.name ? formatName(meta.name) : 'Vasanerd';
    const ogImageUrl = `${SITE_URL}/og/${idpe}`;
    const canonicalUrl = `${SITE_URL}/p/${idpe}`;

    const raceName = meta.raceLabel || 'the race';
    const firstName = displayName.split(' ')[0];
    const title = `${firstName}'s numbers from ${raceName} | vasanerd`;
    const description = `Full race statistics, checkpoint splits and comparison on vasanerd.se`;

    // Replace OG meta tags in the HTML (use replacer functions to avoid $-sequence injection)
    html = html
      .replace(/<title>[^<]*<\/title>/, () => `<title>${esc(title)}</title>`)
      .replace(/(<meta\s+property="og:title"\s+content=")[^"]*"/, (_, p1) => `${p1}${esc(title)}"`)
      .replace(/(<meta\s+property="og:description"\s+content=")[^"]*"/, (_, p1) => `${p1}${esc(description)}"`)
      .replace(/(<meta\s+property="og:image"\s+content=")[^"]*"/, (_, p1) => `${p1}${esc(ogImageUrl)}"`)
      .replace(/(<meta\s+property="og:url"\s+content=")[^"]*"/, (_, p1) => `${p1}${esc(canonicalUrl)}"`)
      .replace(/(<meta\s+name="twitter:title"\s+content=")[^"]*"/, (_, p1) => `${p1}${esc(title)}"`)
      .replace(/(<meta\s+name="twitter:description"\s+content=")[^"]*"/, (_, p1) => `${p1}${esc(description)}"`)
      .replace(/(<meta\s+name="twitter:image"\s+content=")[^"]*"/, (_, p1) => `${p1}${esc(ogImageUrl)}"`)

    return new Response(html, {
      headers: {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'public, max-age=86400, stale-while-revalidate=604800',
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'DENY',
        'Referrer-Policy': 'strict-origin-when-cross-origin',
      },
    });
  },
};

async function getPersonMeta(idpe) {
  const person = await lookupPerson(idpe);
  if (person) return { name: person.name, year: person.year, time: '', raceLabel: person.raceLabel };
  return { name: '', year: '', time: '', raceLabel: '' };
}
