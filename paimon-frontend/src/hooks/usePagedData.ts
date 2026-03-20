import { useState, useCallback } from 'react';
import { useQuery } from '@tanstack/react-query';

interface PagedResult<T> {
  data: T[];
  nextPageToken?: string;
}

interface UsePagedDataOptions<T> {
  queryKey: string[];
  fetcher: (pageToken?: string) => Promise<PagedResult<T>>;
  enabled?: boolean;
}

export function usePagedData<T>({ queryKey, fetcher, enabled = true }: UsePagedDataOptions<T>) {
  const [pageToken, setPageToken] = useState<string | undefined>(undefined);
  const [allTokens, setAllTokens] = useState<(string | undefined)[]>([undefined]);
  const [pageIndex, setPageIndex] = useState(0);

  const query = useQuery({
    queryKey: [...queryKey, pageToken],
    queryFn: () => fetcher(pageToken),
    enabled,
  });

  const goNext = useCallback(() => {
    if (query.data?.nextPageToken) {
      const nextToken = query.data.nextPageToken;
      setPageToken(nextToken);
      setPageIndex((i) => {
        const next = i + 1;
        setAllTokens((tokens) => {
          const updated = [...tokens];
          updated[next] = nextToken;
          return updated;
        });
        return next;
      });
    }
  }, [query.data?.nextPageToken]);

  const goPrev = useCallback(() => {
    if (pageIndex > 0) {
      setPageIndex((i) => {
        const prev = i - 1;
        setPageToken(allTokens[prev]);
        return prev;
      });
    }
  }, [pageIndex, allTokens]);

  return {
    data: query.data?.data ?? [],
    isLoading: query.isLoading,
    error: query.error,
    hasNext: !!query.data?.nextPageToken,
    hasPrev: pageIndex > 0,
    pageIndex,
    goNext,
    goPrev,
    refetch: query.refetch,
  };
}
