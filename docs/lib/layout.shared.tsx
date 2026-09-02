import Image from 'next/image';
import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <span className="flex items-center gap-2 font-semibold tracking-wide">
          <Image src="/logo.png" alt="" width={30} height={30} priority />
          KOMODO
        </span>
      ),
    },
    links: [
      {
        text: 'Quick Start',
        url: '/quick-start',
        active: 'nested-url',
      },
      {
        text: 'Screenshots',
        url: '/screenshots',
        active: 'url',
      },
      {
        text: 'GitHub',
        url: 'https://github.com/neurekadev/komodo',
        external: true,
      },
    ],
    githubUrl: 'https://github.com/neurekadev/komodo',
  };
}
