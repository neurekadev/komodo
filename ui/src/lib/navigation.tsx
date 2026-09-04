import {
  createContext,
  ReactNode,
  useCallback,
  useContext,
  useRef,
} from "react";
import { useLocation, useNavigate } from "react-router-dom";

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
