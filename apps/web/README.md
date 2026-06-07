# Zetta Web

Internal React dashboard for inspecting Zetta collection progress and current
Polymarket data.

## Development

```bash
npm install
npm run dev
```

By default the app calls `/api`, which matches the nginx production deployment. For
local development, Vite proxies `/api` to `http://127.0.0.1:8088`. To use a different
API target, create `apps/web/.env.local`:

```dotenv
ZETTA_API_PROXY_TARGET=https://api-zetta.prophet.zone
```

Then run:

```bash
npm run dev
```

Production builds still use same-origin `/api`; the server nginx config owns that proxy.

## Production Build

```bash
npm run build
```

The generated static files are under `dist/`. The current server deployment serves
them from `/var/www/zetta` and proxies `/api/` to the local product API on
`127.0.0.1:8088`.

## Server Notes

- nginx site config source: `infra/nginx/zetta.prophet.zone.conf`
- public frontend domain: `zetta.prophet.zone`
- API proxy path used by the app: `/api`
- temporary frontend access control: nginx Basic Auth

When DNS is ready, request the HTTPS certificate with:

```bash
certbot --nginx -d zetta.prophet.zone
```
