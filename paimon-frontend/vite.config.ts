import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import type { IncomingMessage, ServerResponse } from 'http';
import httpProxy from 'http-proxy';

// Custom middleware to proxy requests for remote catalogs.
// Requests to /proxy?target=<encoded-url> are forwarded to the target URL,
// bypassing CORS restrictions.
function catalogProxyPlugin() {
  return {
    name: 'catalog-proxy',
    configureServer(server: { middlewares: { use: (fn: (req: IncomingMessage, res: ServerResponse, next: () => void) => void) => void } }) {
      const proxy = httpProxy.createProxyServer({});

      proxy.on('error', (_err, _req, res) => {
        if (res && 'writeHead' in res) {
          (res as ServerResponse).writeHead(502, { 'Content-Type': 'application/json' });
          (res as ServerResponse).end(JSON.stringify({ message: 'Proxy error: target unreachable' }));
        }
      });

      server.middlewares.use((req, res, next) => {
        if (!req.url?.startsWith('/proxy?')) {
          next();
          return;
        }
        const url = new URL(req.url, 'http://localhost');
        const target = url.searchParams.get('target');
        if (!target) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ message: 'Missing target parameter' }));
          return;
        }

        try {
          const targetUrl = new URL(target);
          req.url = targetUrl.pathname + targetUrl.search;
          proxy.web(req, res, {
            target: targetUrl.origin,
            changeOrigin: true,
            secure: false,
          });
        } catch {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ message: 'Invalid target URL' }));
        }
      });
    },
  };
}

export default defineConfig({
  plugins: [react(), catalogProxyPlugin()],
  server: {
    port: 5173,
    proxy: {
      // Local catalog proxy (when baseUrl is empty)
      '/v1': {
        target: 'http://127.0.0.1:8080',
        secure: false,
        changeOrigin: true,
      },
    },
  },
  build: {
    rollupOptions: {
      output: {
        manualChunks: {
          vendor: ['react', 'react-dom', 'react-router-dom'],
          antd: ['antd', '@ant-design/icons'],
        },
      },
    },
  },
});
