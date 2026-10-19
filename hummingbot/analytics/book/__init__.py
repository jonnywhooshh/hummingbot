"""Offline Backpack books and source-labeled references.

Use ``stream.BookStream`` to apply raw snapshots and depth messages for one
market. Use ``reference.BookReference`` to select pre-fill and horizon mids
from the bot and optional sidecar streams. No module starts a subscription,
opens a database, or changes the bot's order book.
"""
