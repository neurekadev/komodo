import {
  createContext,
  ReactNode,
  useCallback,
  useContext,
  useEffect,
  useRef,
} from "react";
import {
  useLocation,
  useNavigate,
  useSearchParams,
} from "react-router-dom";

const InAppHistoryContext = createContext(false);

function browserHistoryIndex() {
  const index = window.history.state?.idx;
  return typeof index === "number" ? index : undefined;
}

/**
 * Tracks history entries created after this document loaded. This deliberately
 * ignores entries from before a reload, direct link, or newly opened tab.
 */
export function InAppHistoryProvider({ children }: { children: ReactNode }) {
  const location = useLocation();
  const initial = useRef({
    index: browserHistoryIndex(),
    key: location.key,
  });
  const currentIndex = browserHistoryIndex();
  const hasInAppPrevious =
    initial.current.index !== undefined && currentIndex !== undefined
      ? currentIndex > initial.current.index
      : location.key !== initial.current.key;

  return (
    <InAppHistoryContext.Provider value={hasInAppPrevious}>
      {children}
    </InAppHistoryContext.Provider>
  );
}

export function useHistoryAwareBack(fallback: string) {
  const navigate = useNavigate();
  const hasInAppPrevious = useContext(InAppHistoryContext);
  return useCallback(() => {
    if (hasInAppPrevious) {
      navigate(-1);
    } else {
      navigate(fallback, { replace: true });
    }
  }, [fallback, hasInAppPrevious, navigate]);
}

export function useUrlBackedTab<T extends string>(
  parameter: string,
  values: readonly T[],
  storedValue: T,
  setStoredValue: (value: T) => void,
): [T, (value: T) => void] {
  const [searchParams, setSearchParams] = useSearchParams();
  const requested = searchParams.get(parameter)?.toLowerCase();
  const urlValue = values.find(
    (value) => value.toLowerCase() === requested,
  );
  const validStoredValue = values.includes(storedValue)
    ? storedValue
    : values[0];
  const value = urlValue ?? validStoredValue;

  useEffect(() => {
    if (urlValue && urlValue !== storedValue) {
      setStoredValue(urlValue);
    }
  }, [setStoredValue, storedValue, urlValue]);

  const setValue = useCallback(
    (next: T) => {
      setStoredValue(next);
      setSearchParams(
        (current) => {
          const updated = new URLSearchParams(current);
          updated.set(parameter, next.toLowerCase());
          return updated;
        },
        { replace: true },
      );
    },
    [parameter, setSearchParams, setStoredValue],
  );

  return [value, setValue];
}

export function serverDockerPath(
  serverId: string,
  resource: "containers" | "images" | "volumes" | "networks",
) {
  return `/servers/${serverId}?tab=docker&docker=${resource}`;
}
