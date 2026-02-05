# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, please email **debusinha2009@gmail.com** with:

- A description of the vulnerability
- Steps to reproduce the issue
- Any potential impact you've identified

You should receive a response within 72 hours. We will work with you to understand the issue and coordinate a fix before any public disclosure.

## Scope

This project automates Databricks workspace operations involving:

- **Secret scopes** — stores target workspace credentials (host URL, personal access tokens)
- **Delta Sharing** — transfers data between workspaces
- **Serving endpoints** — deploys models accessible via API

### In scope

- Credential exposure through logs, error messages, or misconfiguration
- Insecure handling of personal access tokens or secret scope values
- Unintended data access through Delta Sharing misconfigurations
- Privilege escalation through job or endpoint configurations

### Out of scope

- Vulnerabilities in Databricks platform services themselves (report to [Databricks Security](https://www.databricks.com/trust/security))
- Issues requiring physical access to infrastructure
- Social engineering attacks

## Best Practices for Users

- Use scoped personal access tokens with minimum required permissions
- Rotate tokens regularly
- Restrict secret scope access using ACLs
- Review `databricks.yml` variable values before deploying to production
- Use separate workspaces (not just profiles) for production deployments
