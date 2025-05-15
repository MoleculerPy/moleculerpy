# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.14.x  | :white_check_mark: |
| < 0.14  | :x:                |

## Reporting a Vulnerability

If you discover a security vulnerability within MoleculerPy, please send an email to explosivebit@gmail.com.

**Please do not report security vulnerabilities through public GitHub issues.**

You should receive a response within 48 hours. If for some reason you do not, please follow up via email to ensure we received your original message.

Please include the following information (as much as you can provide):

* Type of issue (e.g., buffer overflow, SQL injection, cross-site scripting, etc.)
* Full paths of source file(s) related to the manifestation of the issue
* The location of the affected source code (tag/branch/commit or direct URL)
* Any special configuration required to reproduce the issue
* Step-by-step instructions to reproduce the issue
* Proof-of-concept or exploit code (if possible)
* Impact of the issue, including how an attacker might exploit it

We will acknowledge receipt of your vulnerability report and send you regular updates about our progress.

## Security Best Practices

When using MoleculerPy in production:

1. **Keep dependencies updated** - Regularly update MoleculerPy and its dependencies
2. **Use secure transporters** - Enable TLS for NATS/Redis connections in production
3. **Validate input** - Always validate action parameters
4. **Use namespaces** - Isolate services using namespaces
5. **Monitor logs** - Set up proper logging and monitoring
