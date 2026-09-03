# Copy Database Utility

`km database copy` copies documents between configured MongoDB-compatible databases. It does not convert upstream Komodo data into this fork's schema. Komodo 3.0.0+ does not support upstream migrations or promise backward compatibility.

Use `docker exec -it komodo km database copy --help` to inspect the available options. Before copying a fork database, back it up, select a separate target, and keep the original database unchanged. Verify the target with the same fork version before using it; if verification fails, keep using the original database and preserve the failed copy for diagnosis.

See [Backup and Restore](/administration/backups) for the supported backup workflow.
