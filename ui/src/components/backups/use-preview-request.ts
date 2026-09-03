import { useCallback, useLayoutEffect, useRef, useState } from "react";

/** A preview belongs to one request and one unchanged form/open context. */
export function usePreviewRequest<T>(contextKey: string) {
  const generation = useRef(0);
  const currentContext = useRef(contextKey);
  const [result, setResult] = useState<{
    contextKey: string;
    generation: number;
    value: T;
  }>();

  const invalidate = useCallback(() => {
    generation.current += 1;
    setResult(undefined);
  }, []);

  useLayoutEffect(() => {
    currentContext.current = contextKey;
    invalidate();
    return () => {
      // Also reject requests that finish after unmount or a context change.
      generation.current += 1;
    };
  }, [contextKey, invalidate]);

  const begin = () => {
    const requestGeneration = ++generation.current;
    setResult(undefined);
    return (value: T) => {
      if (
        currentContext.current === contextKey &&
        generation.current === requestGeneration
      ) {
        setResult({ contextKey, generation: requestGeneration, value });
      }
    };
  };

  return {
    // Hide an obsolete result during render, before layout-effect invalidation.
    preview:
      result && result.contextKey === contextKey &&
      result.generation === generation.current
        ? result.value
        : undefined,
    begin,
    invalidate,
  };
}
