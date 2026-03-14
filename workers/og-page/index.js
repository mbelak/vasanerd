/**
 * Cloudflare Worker: vasanerd.se/p/{idpe}
 *
 * Fetches the SPA index.html and injects personalized OG meta tags.
 * Crawlers and browsers both get the full page with correct metadata.
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
      fetch(`${SITE_URL}/index.html`),
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
        'Cache-Control': 'public, max-age=3600',
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'DENY',
        'Referrer-Policy': 'strict-origin-when-cross-origin',
      },
    });
  },
};

async function getPersonMeta(idpe) {
  // Search across all race persons.json files in parallel
  const results = await Promise.all(
    RACES.map(async (r) => {
      try {
        const resp = await fetch(`${SITE_URL}/data/${r.key}/persons.json`);
        if (!resp.ok) return null;
        const persons = await resp.json();
        if (persons[idpe]) return { data: persons[idpe], raceLabel: r.label };
      } catch (e) { console.error(`getPersonMeta ${r.key}:`, e); }
      return null;
    })
  );
  const found = results.find(Boolean);
  if (found) {
    return {
      name: found.data.namn || '',
      year: found.data.years && found.data.years.length
        ? String(Math.max(...found.data.years))
        : '',
      time: '',
      raceLabel: found.raceLabel,
    };
  }
  return { name: '', year: '', time: '', raceLabel: '' };
}
