'use client';

import type { ReactNode } from 'react';
import { RootProvider } from 'fumadocs-ui/provider/next';
import SearchDialog from '@/components/search';

export function Provider({ children }: { children: ReactNode }) {
  return (
    <RootProvider
      search={{ SearchDialog }}
      theme={{
        defaultTheme: 'dark',
        enableSystem: false,
        themes: ['light', 'dark'],
      }}
    >
      {children}
    </RootProvider>
  );
}
