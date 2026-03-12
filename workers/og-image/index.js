/**
 * Cloudflare Worker: vasanerd.se/og/{idpe}
 *
 * Generates personalized OG images (SVG) with the person's name.
 * Uses CF Cache API for caching (no R2/KV needed).
 */

const SITE_URL = 'https://vasanerd.se';
const ALLOWED_ORIGINS = ['https://vasanerd.se', 'http://localhost:8000'];
const IDPE_RE = /^[a-zA-Z0-9]{8,32}$/;
const RACES = [
  { key: 'vasaloppet', label: 'Vasaloppet' },
  { key: 'tjejvasan', label: 'Tjejvasan' },
  { key: 'ultravasan', label: 'Ultravasan' },
  { key: 'oppet_spar_mandag', label: 'Öppet Spår måndag' },
  { key: 'oppet_spar_sondag', label: 'Öppet Spår söndag' },
];
const CACHE_TTL = 86400; // 24 hours

function corsHeaders(req) {
  const origin = req.headers.get('Origin') || '';
  const allowed = ALLOWED_ORIGINS.includes(origin) ? origin : null;
  const headers = {
    'Vary': 'Origin',
    'X-Content-Type-Options': 'nosniff',
  };
  if (allowed) {
    headers['Access-Control-Allow-Origin'] = allowed;
    headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS';
    headers['Access-Control-Allow-Headers'] = 'Content-Type';
  }
  return headers;
}

function esc(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function formatName(raw) {
  const clean = raw.replace(/\s*\(\w+\)/, '');
  const parts = clean.split(/,\s*/);
  return parts.length === 2 ? parts[1] + ' ' + parts[0] : clean;
}

async function findPerson(idpe) {
  const results = await Promise.all(
    RACES.map(async (r) => {
      try {
        const resp = await fetch(`${SITE_URL}/data/${r.key}/persons.json`);
        if (!resp.ok) return null;
        const persons = await resp.json();
        if (persons[idpe]) return persons[idpe];
      } catch (e) { console.error(`findPerson ${r.key}:`, e); }
      return null;
    })
  );
  return results.find(Boolean) || null;
}

async function getBackgroundBase64() {
  const resp = await fetch(`${SITE_URL}/og-bg.jpg`);
  if (!resp.ok) throw new Error('Failed to fetch background image');
  const buf = await resp.arrayBuffer();
  const bytes = new Uint8Array(buf);
  let binary = '';
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}

function generateSvg(bgB64, displayName) {
  const W = 1200, H = 630;
  const firstName = displayName.split(' ')[0];
  const cta = `Check out ${firstName}\u2019s numbers from the race`;

  return `<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">
  <image href="data:image/jpeg;base64,${bgB64}" width="${W}" height="${H}" preserveAspectRatio="xMidYMid slice"/>
  <text x="600" y="100" fill="#ffffff" font-family="system-ui, -apple-system, 'Segoe UI', sans-serif" font-size="52" font-weight="700" letter-spacing="-1" text-anchor="middle">${esc(displayName)}</text>
  <rect x="${600 - (cta.length * 21 + 80) / 2}" y="178" width="${cta.length * 21 + 80}" height="68" rx="34" fill="none" stroke="#ffffffcc" stroke-width="4"/>
  <text x="600" y="227" fill="#ffffffcc" font-family="system-ui, -apple-system, sans-serif" font-size="42" font-weight="600" text-anchor="middle">${esc(cta)}</text>
</svg>`;
}

export default {
  async fetch(request, env, ctx) {
    const cors = corsHeaders(request);

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: cors });
    }

    if (request.method !== 'GET') {
      return new Response('Method not allowed', { status: 405, headers: cors });
    }

    // Parse idpe from /og/{idpe}
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/og\/([a-zA-Z0-9]{8,32})$/);
    if (!match) {
      return new Response('Missing or invalid idpe', { status: 400, headers: cors });
    }
    const idpe = match[1];

    // Check CF Cache API
    const cache = caches.default;
    const cacheKey = new Request(url.toString(), request);
    const cachedResponse = await cache.match(cacheKey);
    if (cachedResponse) {
      return cachedResponse;
    }

    // Generate on-demand
    try {
      const person = await findPerson(idpe);
      if (!person) {
        return new Response('Person not found', { status: 404, headers: cors });
      }

      const displayName = formatName(person.namn || 'Okänd');
      const bgB64 = await getBackgroundBase64();
      const svg = generateSvg(bgB64, displayName);

      const response = new Response(svg, {
        headers: {
          ...cors,
          'Content-Type': 'image/svg+xml',
          'Content-Security-Policy': "default-src 'none'; style-src 'unsafe-inline'",
          'Cache-Control': `public, max-age=${CACHE_TTL}`,
        },
      });

      // Store in CF Cache (non-blocking)
      ctx.waitUntil(cache.put(cacheKey, response.clone()));

      return response;
    } catch (e) {
      console.error('og-image generate error:', e);
      return new Response('Failed to generate image', { status: 500, headers: cors });
    }
  },
};
