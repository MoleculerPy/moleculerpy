"""Multi-node cluster examples for MoleculerPy.

This package contains examples for running a multi-node MoleculerPy cluster
with NATS transporter and resilience middleware testing.

Usage:
    Terminal 1 (Server):
        python -m examples.cluster.server_beta

    Terminal 2 (Client):
        python -m examples.cluster.client_alpha

    Terminal 3 (Optional, for load balancing):
        python -m examples.cluster.server_gamma
"""
