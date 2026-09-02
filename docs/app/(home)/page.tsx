import Image from 'next/image';
import Link from 'next/link';
import { ArrowRight, Boxes, ShieldCheck, Workflow } from 'lucide-react';

const capabilities = [
  {
    title: 'Deploy',
    description: 'Run containers and Compose stacks across one server or a fleet.',
    href: '/features/stacks',
    icon: Boxes,
  },
  {
    title: 'Automate',
    description: 'Combine procedures, schedules, webhooks, and resource syncs.',
    href: '/features/automation-overview',
    icon: Workflow,
  },
  {
    title: 'Operate safely',
    description: 'Control access, backups, credentials, and deployment trust boundaries.',
    href: '/administration/security-and-trust',
    icon: ShieldCheck,
  },
] as const;

export default function HomePage() {
  return (
    <main className="relative flex flex-1 flex-col overflow-hidden">
      <div className="hero-grid pointer-events-none absolute inset-0 -z-10" />
      <section className="mx-auto flex w-full max-w-6xl flex-col items-center px-6 py-20 text-center sm:py-28 lg:py-36">
        <Image src="/logo.png" alt="Komodo" width={104} height={104} priority />
        <p className="mt-8 text-sm font-semibold uppercase tracking-[0.24em] text-komodo-400">
          Self-hosted deployment and automation
        </p>
        <h1 className="mt-4 max-w-4xl text-balance text-4xl font-bold tracking-tight sm:text-6xl">
          Run your servers, stacks, builds, and workflows from one place.
        </h1>
        <p className="mt-6 max-w-2xl text-balance text-lg text-fd-muted-foreground sm:text-xl">
          Start with a supported Compose deployment, connect a server, and ship your first stack.
          Operator details are there when you need them.
        </p>
        <div className="mt-10 flex w-full flex-col justify-center gap-3 sm:w-auto sm:flex-row">
          <Link
            href="/quick-start"
            className="inline-flex min-h-11 items-center justify-center gap-2 rounded-lg bg-fd-primary px-6 py-3 font-semibold text-fd-primary-foreground outline-none transition hover:opacity-90 focus-visible:ring-2 focus-visible:ring-fd-ring focus-visible:ring-offset-2"
          >
            Quick Start <ArrowRight aria-hidden="true" size={18} />
          </Link>
          <Link
            href="/screenshots"
            className="inline-flex min-h-11 items-center justify-center rounded-lg border border-fd-border bg-fd-card px-6 py-3 font-semibold outline-none transition hover:bg-fd-accent focus-visible:ring-2 focus-visible:ring-fd-ring focus-visible:ring-offset-2"
          >
            See the interface
          </Link>
        </div>
      </section>
      <section aria-labelledby="capabilities" className="mx-auto w-full max-w-6xl px-6 pb-20 sm:pb-28">
        <h2 id="capabilities" className="sr-only">What you can do with Komodo</h2>
        <div className="grid gap-4 md:grid-cols-3">
          {capabilities.map(({ title, description, href, icon: Icon }) => (
            <Link
              key={href}
              href={href}
              className="group rounded-xl border border-fd-border bg-fd-card/80 p-6 outline-none backdrop-blur transition hover:border-komodo-500 hover:bg-fd-accent focus-visible:ring-2 focus-visible:ring-fd-ring"
            >
              <Icon aria-hidden="true" className="text-komodo-400" />
              <h3 className="mt-5 text-lg font-semibold">{title}</h3>
              <p className="mt-2 text-sm leading-6 text-fd-muted-foreground">{description}</p>
              <span className="mt-5 inline-flex items-center gap-1 text-sm font-medium text-fd-primary">
                Learn more <ArrowRight aria-hidden="true" size={15} className="transition group-hover:translate-x-0.5" />
              </span>
            </Link>
          ))}
        </div>
      </section>
    </main>
  );
}
