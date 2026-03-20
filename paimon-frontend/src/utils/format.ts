export function formatTimestamp(millis: number | undefined): string {
  if (!millis || millis <= 0) return '-';
  return new Date(millis).toLocaleString();
}

export function formatBytes(bytes: number | undefined): string {
  if (bytes === undefined || bytes < 0) return '-';
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const value = bytes / Math.pow(1024, i);
  return `${value.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

export function formatNumber(n: number | undefined): string {
  if (n === undefined) return '-';
  return n.toLocaleString();
}
