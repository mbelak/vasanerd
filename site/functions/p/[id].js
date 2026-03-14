/**
 * Cloudflare Pages Function: /p/{idpe}
 *
 * Serves HTML with OG meta tags for social crawlers.
 * Browsers get redirected to the SPA at /#person-{idpe}.
 * OG image is generated on-demand by the og-image edge function.
 */

const SITE_URL = 'https://vasanerd.se';
const IDPE_RE = /^[a-zA-Z0-9]{8,32}$/;
const RACES = [
  { key: 'vasaloppet', label: 'Vasaloppet' },
  { key: 'tjejvasan', label: 'Tjejvasan' },
  { key: 'ultravasan', label: 'Ultravasan' },
  { key: 'oppet_spar_mandag', label: 'Öppet Spår måndag' },
  { key: 'oppet_spar_sondag', label: 'Öppet Spår söndag' },
  { key: 'birken', label: 'Birkebeinerrennet' },
];

function esc(s) {
  return String(s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

export async function onRequestGet({ params }) {
  const idpe = params.id;
  if (!idpe || !IDPE_RE.test(idpe)) {
    return new Response('Invalid ID', { status: 400 });
  }

  // Search across all race persons.json files in parallel
  let name = '';
  let year = '';
  let time = '';
  let raceLabel = '';

  const results = await Promise.all(
    RACES.map(async (r) => {
      try {
        const resp = await fetch(`${SITE_URL}/data/${r.key}/persons.json`);
        if (!resp.ok) return null;
        const persons = await resp.json();
        if (persons[idpe]) return { data: persons[idpe], raceLabel: r.label, raceKey: r.key };
      } catch {}
      return null;
    })
  );
  const found = results.find(Boolean);
  let raceKey = '';
  if (found) {
    name = found.data.namn || '';
    year = found.data.years && found.data.years.length ? String(Math.max(...found.data.years)) : '';
    raceLabel = found.raceLabel;
    raceKey = found.raceKey;
  }

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
      'Cache-Control': 'public, max-age=3600',
      'X-Content-Type-Options': 'nosniff',
    },
  });
}
