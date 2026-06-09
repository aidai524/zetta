# Zetta Analytics

User-facing data platform for market, wallet, flow, and anomaly analytics.

## Development

```bash
npm install
npm run dev
```

The app calls `/api` by default. In local development, Vite proxies `/api` to
`http://127.0.0.1:8088`. Use `apps/analytics/.env.local` to point at a different
API service:

```dotenv
ZETTA_API_PROXY_TARGET=http://127.0.0.1:8091
```

## Production Build

```bash
npm run build
```

The generated static files are under `dist/`.
