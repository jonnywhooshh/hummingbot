from decimal import Decimal


class Prices:
    def __init__(self, values=None):
        self.values = values or {}

    def mid(self, token, t):
        return self.values.get((token, t), Decimal("100") if token == "SOL" else None)
