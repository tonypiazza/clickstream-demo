# ==============================================================================
# Services Package
# ==============================================================================
"""
Service health check implementations.

Environment-specific implementations of ServiceHealthCheck:
- local.py: Direct connection checks for local/Docker environments
- aiven.py: REST API checks for Aiven-hosted environments

Factory function: clickstream.utils.config.get_health_checker()
"""
