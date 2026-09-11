# Security policy

## Report a vulnerability

Use GitHub's private
[Report a vulnerability](https://github.com/liran/sink/security/advisories/new)
form. Include the affected Sink version or commit, reproduction steps, impact,
and relevant deployment settings with credentials and private data removed.
Please keep exploit details out of public issues while a report is being
investigated.

## Versions

Use the [latest release](https://github.com/liran/sink/releases/latest) when
checking whether an issue is still present. Older releases do not have a
guaranteed security backport schedule. Report suspected vulnerabilities even
if you cannot reproduce them on the latest release.

## Deployment guidance

Review the [configuration reference](docs/configuration.md) and
[reliability guide](docs/reliability.md) for dependency access, resource limits,
health checks, and durability settings. The Docker Compose quickstart is a
local development environment.
