"""A local operator console for BoundFlow.

`boundflow ui` serves a page on localhost that shows the fleet and lets an operator
act on what needs a human: approval gates, input gates, suspend and resume. It is a
client of `ControlPlaneClient` and nothing more — no storage, no identity, no state
of its own. Whatever the API key can reach is what the console can show.

Deliberately not here: create, set-config, and the policy setters. Authoring a
lifecycle policy through a web form is worse than writing the JSON, and the authoring
screens are what turn an operator console into a product surface to maintain.
"""

from .server import serve

__all__ = ["serve"]
