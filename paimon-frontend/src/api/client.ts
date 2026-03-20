import axios from 'axios';
import type { ErrorResponse } from './types';
import {
  type CatalogConfig,
  getPrefix,
} from '../store/catalogStore';

let currentCatalog: CatalogConfig | null = null;

export function setCurrentCatalog(catalog: CatalogConfig | null) {
  currentCatalog = catalog;
}

export function getCurrentPrefix(): string {
  return getPrefix(currentCatalog);
}

const apiClient = axios.create({
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Dynamically route requests:
// - Local catalog (baseUrl empty): request /v1/... directly (same origin)
// - Remote catalog: request /proxy?target=<baseUrl>/v1/... (server-side proxy)
apiClient.interceptors.request.use((config) => {
  const baseUrl = currentCatalog?.baseUrl?.replace(/\/+$/, '');
  const path = config.url || '';

  if (baseUrl) {
    // Remote catalog: route through server-side proxy
    const targetUrl = `${baseUrl}/v1${path}`;
    config.baseURL = '';
    config.url = `/proxy?target=${encodeURIComponent(targetUrl)}`;
  } else {
    // Local catalog: direct same-origin request
    config.baseURL = '/v1';
  }
  return config;
});

apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.data) {
      const errData = error.response.data as ErrorResponse;
      return Promise.reject(new Error(errData.message || 'Unknown error'));
    }
    return Promise.reject(error);
  }
);

export default apiClient;
