"""Cognis Trade — productization surface for the cognis-trade fork.

This package is the *only* Cognis-specific code in the fork. Everything else
in ``hummingbot/`` is upstream Hummingbot, untouched. The contract:

- ``bridge_client.BridgeClient`` — async HTTP client to Cognis Bridge for
  fetching tenant config (decrypted exchange keys, risk caps, strategies)
  and posting back events / paper-trading results.
- ``risk_guards.RiskGuards`` — defense-in-depth caps enforced *here* on the
  bot side (Bridge is the source of truth; this is belt-and-suspenders so
  a Bridge bug can't accidentally let a strategy exceed plan caps).
- ``branding.cognis_brand`` — env-driven Cognis brand strings (name, support
  email, banner colours) consumed by the CLI banner + log prefixes.
- ``branding.banner`` — Cognis CLI banner printer; invoked from
  ``bin/cognis_trade.py`` before deferring to upstream's startup.

NEVER import ``openai``, ``anthropic``, ``litellm``, or any LLM SDK from this
package. Strategy-suggestion LLM calls are owned by Cognis Bridge; the bot
itself never talks to an LLM provider.
"""

__all__ = ["bridge_client", "risk_guards", "branding"]
