/**
 * Cloudflare Pages Function: /p/{idpe}
 *
 * Serves HTML with OG meta tags for social crawlers.
 * Browsers get redirected to the SPA at /#person-{idpe}.
 * OG image is generated on-demand by the og-image edge function.
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
  return String(s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

export async function onRequestGet({ params }) {
  const idpe = params.id;
  if (!idpe || !IDPE_RE.test(idpe)) {
    return new Response('Invalid ID', { status: 400 });
  }

  const person = await lookupPerson(idpe);
  const name = person ? person.name : '';
  const raceLabel = person ? person.raceLabel : '';
  const raceKey = person ? person.raceKey : '';

  // Format display name
  let displayName = 'Vasanerd';
  if (name) {
    const clean = name.replace(/\s*\(\w+\)/, '');
    const parts = clean.split(/,\s*/);
    displayName = parts.length === 2 ? parts[1] + ' ' + parts[0] : clean;
  }

  // OG image URL — always point to the edge function which generates on-demand
  const ogImageUrl = `${SITE_URL}/og/${idpe}`;
  const raceHash = raceKey && raceKey !== 'vasaloppet' ? raceKey + '-' : '';
  const personUrl = `${SITE_URL}/#${raceHash}person-${idpe}`;
  const canonicalUrl = `${SITE_URL}/p/${idpe}`;

  const raceName = raceLabel || 'the race';
  const firstName = displayName.split(' ')[0];
  const title = `${firstName}'s numbers from ${raceName} | vasanerd`;
  const description = `Full race statistics, checkpoint splits and comparison on vasanerd.se`;

  const html = `<!DOCTYPE html>
<html lang="sv">
<head>
  <meta charset="utf-8">
  <title>${esc(title)}</title>
  <meta property="og:type" content="website">
  <meta property="og:title" content="${esc(title)}">
  <meta property="og:description" content="${esc(description)}">
  <meta property="og:image" content="${esc(ogImageUrl)}">
  <meta property="og:image:width" content="1200">
  <meta property="og:image:height" content="630">
  <meta property="og:url" content="${esc(canonicalUrl)}">
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="${esc(title)}">
  <meta name="twitter:description" content="${esc(description)}">
  <meta name="twitter:image" content="${esc(ogImageUrl)}">
  <link rel="canonical" href="${esc(canonicalUrl)}">
  <meta http-equiv="refresh" content="0;url=${esc(personUrl)}">
</head>
<body>
  <p>Omdirigerar till <a href="${esc(personUrl)}">${esc(title)}</a>...</p>
</body>
</html>`;

  return new Response(html, {
    headers: {
      'Content-Type': 'text/html; charset=utf-8',
      // Unknown idpe: cache briefly so a freshly imported person shows up soon
      'Cache-Control': person ? 'public, max-age=86400, stale-while-revalidate=604800' : 'public, max-age=300',
      'X-Content-Type-Options': 'nosniff',
    },
  });
}
