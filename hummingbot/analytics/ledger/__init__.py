"""Offline ledger API. No trading code imports or activates this package.

Construct Opening and a Mids provider, then Ledger(opening, mids, ...).
Apply Fill, FundingPayment, and Transfer in venue order strictly after t_open.
An event at or before t_open raises ValueError. value(t) samples the current
event state without advancing its cursor. EquityPoint includes cumulative
external_flows and net_pnl for historical drawdown observations.
attribute(ledger, t) returns the Q1 split. drawdown consumes ordered
(venue_time, cumulative_net_pnl) points with t_open or an opening zero.
All financial inputs are finite Decimal values. Timestamps use one integer
unit selected by the caller. Missing Fill.fee_token and Transfer.token mean
the ledger quote. Token transfers retain quantities and their quote value
at transfer time. An unavailable transfer mid leaves external_flows and
net_pnl unknown, even when ending equity can be valued.

Attribution.fees_by_token is net of rebates. paid_fees_by_token reports gross
costs separately from rebates_by_token. fee_quote_values and
fee_valuation_basis retain each token's fill-time valuation or its missing
mid reason. instrument can select a perpetual mid separately from token mids.
base_valuation_exposure includes base-token fees and transfers without
changing perpetual contract position.

Finite accounting sums and products use precision derived from input digit
width and event count in a Decimal context the ledger copies from the
caller's at construction and never writes back. Cost lots retain rational
total costs. Recurring cost-basis and ratio outputs round only on conversion
to Decimal, without rounding the stored costs.
"""

from hummingbot.analytics.ledger.attribution import Attribution, Drawdown, attribute, drawdown
from hummingbot.analytics.ledger.equity import EquityPoint, Ledger
from hummingbot.analytics.ledger.model import Fill, FundingPayment, Mids, Opening, Transfer

__all__ = ["Attribution", "Drawdown", "EquityPoint", "Fill", "FundingPayment", "Ledger", "Mids", "Opening", "Transfer",
           "attribute", "drawdown"]
