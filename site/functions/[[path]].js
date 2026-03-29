/**
 * Cloudflare Pages Function: catch-all for /{name_slug}/{idpe} person URLs.
 *
 * Serves HTML with OG meta tags for social crawlers.
 * Browsers get redirected to the SPA which handles routing client-side.
 * Falls through to static assets for non-person URLs.
 */

const SITE_URL = 'https://vasanerd.se';
const IDPE_RE = /^[a-zA-Z0-9]{8,32}$/;
const SLUG_RE = /^[a-z0-9_]+$/;
const RACES = [
  { key: 'vasaloppet', label: 'Vasaloppet' },
  { key: 'tjejvasan', label: 'Tjejvasan' },
  { key: 'ultravasan', label: 'Ultravasan' },
  { key: 'oppet_spar_mandag', label: 'Öppet Spår måndag' },
  { key: 'oppet_spar_sondag', label: 'Öppet Spår söndag' },
  { key: 'birken', label: 'Birkebeinerrennet' },
  { key: 'nsl', label: 'Nordenskiöldsloppet' },
  { key: 'lofsdalen_epic', label: 'Lofsdalen Epic' },
];

// Known static directories that should NOT be handled as person URLs
const STATIC_DIRS = ['data', 'og', 'p', 'functions', 'assets', '_redirects'];

function esc(s) {
  return String(s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

export async function onRequestGet({ params, next }) {
  const parts = params.path;

  // Only handle /{slug}/{idpe} pattern (exactly 2 path segments)
  if (!parts || parts.length !== 2) return next();

  const [slug, idpe] = parts;

  // Skip known static directories
  if (STATIC_DIRS.includes(slug)) return next();

  // Validate format
  if (!SLUG_RE.test(slug) || !IDPE_RE.test(idpe)) return next();

  // Search across all race persons.json files in parallel
  let name = '';
  let raceLabel = '';
  let raceKey = '';

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
  if (found) {
    name = found.data.namn || '';
    raceLabel = found.raceLabel;
    raceKey = found.raceKey;
  }

  // If not found as a person, fall through to static assets
  if (!name) return next();

  // Format display name
  const clean = name.replace(/\s*\(\w+\)/, '');
  const nameParts = clean.split(/,\s*/);
  const displayName = nameParts.length === 2 ? nameParts[1] + ' ' + nameParts[0] : clean;

  const ogImageUrl = `${SITE_URL}/og/${idpe}`;
  const canonicalUrl = `${SITE_URL}/${slug}/${idpe}`;
  const raceHash = raceKey ? '#' + raceKey : '';
  const spaUrl = `${SITE_URL}/${slug}/${idpe}${raceHash}`;

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
  <meta http-equiv="refresh" content="0;url=${esc(spaUrl)}">
</head>
<body>
  <p>Redirecting to <a href="${esc(spaUrl)}">${esc(title)}</a>...</p>
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
