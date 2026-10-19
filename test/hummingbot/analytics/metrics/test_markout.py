from decimal import Decimal
from unittest import TestCase

from hummingbot.analytics.book.reference import BookReference
from hummingbot.analytics.book.stream import BookStream
from hummingbot.analytics.metrics.markout import compute_markout


class MarkoutTests(TestCase):
    def reference(self, *, run_end_ns=30, gap=False, clock_basis="venue"):
        stream = BookStream(clock_basis=clock_basis)
        stream.apply_snapshot({"lastUpdateId": 10, "bids": [["99", "1"]], "asks": [["101", "1"]]},
                              timestamp_ns=0)
        stream.apply_diff({"U": 11, "u": 11, "b": [], "a": []}, timestamp_ns=10)
        stream.apply_diff({"U": 12, "u": 12, "b": [], "a": []}, timestamp_ns=20)
        uid = 14 if gap else 13
        stream.apply_diff({"U": uid, "u": uid, "b": [["99", "0"], ["97", "1"]],
                           "a": [["101", "0"], ["99", "1"]]}, timestamp_ns=30)
        return BookReference(stream, run_end_ns=run_end_ns).observe(15, 15)

    def test_criterion_14_buy(self):
        result = compute_markout(side="buy", quantity=Decimal("1"), price=Decimal("99"),
                                 reference=self.reference(), fee_quote=Decimal("0.01"))
        self.assertEqual("valid", result.reference.status)
        self.assertEqual(Decimal("100"), result.edge_bps)
        self.assertEqual(Decimal("-200"), result.drift_bps)
        self.assertEqual(Decimal("-100"), result.markout_bps)
        self.assertEqual(Decimal("1"), result.edge_value)
        self.assertEqual(Decimal("-2"), result.drift_value)
        self.assertEqual(Decimal("-1"), result.markout_value)
        self.assertEqual(Decimal("-101"), result.fee_adjusted_markout_bps)
        self.assertEqual(Decimal("-1.01"), result.fee_adjusted_markout_value)
        self.assertEqual("venue", result.clock_basis)

    def test_criterion_14_sell(self):
        result = compute_markout(side="sell", quantity=Decimal("1"), price=Decimal("101"),
                                 reference=self.reference(), fee_quote=Decimal("0.01"))
        self.assertEqual(Decimal("100"), result.edge_bps)
        self.assertEqual(Decimal("200"), result.drift_bps)
        self.assertEqual(Decimal("300"), result.markout_bps)
        self.assertEqual(Decimal("1"), result.edge_value)
        self.assertEqual(Decimal("2"), result.drift_value)
        self.assertEqual(Decimal("3"), result.markout_value)
        self.assertEqual(Decimal("299"), result.fee_adjusted_markout_bps)
        self.assertEqual(Decimal("2.99"), result.fee_adjusted_markout_value)

    def test_quantity_scales_value_not_basis_points_and_rebates_add_value(self):
        result = compute_markout(side="buy", quantity=Decimal("2"), price=Decimal("99"),
                                 reference=self.reference(), fee_quote=Decimal("-0.000000123"))
        self.assertEqual(Decimal("-100"), result.markout_bps)
        self.assertEqual(Decimal("2"), result.edge_value)
        self.assertEqual(Decimal("-4"), result.drift_value)
        self.assertEqual(Decimal("-2"), result.markout_value)
        self.assertEqual(Decimal("-1.999999877"), result.fee_adjusted_markout_value)
        self.assertEqual(Decimal("-99.99999385"), result.fee_adjusted_markout_bps)
        self.assertEqual(result.edge_bps + result.drift_bps, result.markout_bps)
        self.assertEqual(result.edge_value + result.drift_value, result.markout_value)

    def test_missing_fee_is_not_zero(self):
        result = compute_markout(side="buy", quantity=Decimal("1"), price=Decimal("99"),
                                 reference=self.reference())
        self.assertEqual(Decimal("-100"), result.markout_bps)
        self.assertIsNone(result.fee_adjusted_markout_value)
        self.assertIsNone(result.fee_adjusted_markout_bps)
        free = compute_markout(side="buy", quantity=Decimal("1"), price=Decimal("99"),
                               reference=self.reference(), fee_quote=Decimal("0"))
        self.assertEqual(free.markout_bps, free.fee_adjusted_markout_bps)

    def test_invalid_and_pending_horizons_keep_known_edge_but_not_markout(self):
        for reference, status in ((self.reference(gap=True), "invalid"),
                                  (self.reference(run_end_ns=20), "pending")):
            with self.subTest(status=status):
                result = compute_markout(side="buy", quantity=Decimal("1"), price=Decimal("99"),
                                         reference=reference, fee_quote=Decimal("0"))
                self.assertEqual(status, result.reference.status)
                self.assertEqual(Decimal("100"), result.edge_bps)
                self.assertIsNone(result.drift_bps)
                self.assertIsNone(result.markout_bps)
                self.assertIsNone(result.markout_value)
                self.assertIsNone(result.fee_adjusted_markout_bps)
                self.assertIsNone(result.fee_adjusted_markout_value)

    def test_unknown_fill_has_no_edge_or_markout(self):
        reference = BookReference(BookStream(), run_end_ns=30).observe(15, 15)
        result = compute_markout(side="buy", quantity=Decimal("1"), price=Decimal("99"), reference=reference)
        self.assertEqual("book_unknown", result.reference.reason)
        self.assertIsNone(result.edge_bps)
        self.assertIsNone(result.edge_value)
        self.assertIsNone(result.drift_value)
        self.assertIsNone(result.markout_value)

    def test_receipt_clock_label_survives_calculation(self):
        result = compute_markout(side="buy", quantity=Decimal("1"), price=Decimal("99"),
                                 reference=self.reference(clock_basis="receipt"))
        self.assertEqual("receipt", result.clock_basis)

    def test_financial_inputs_must_be_exact_positive_decimals(self):
        reference = self.reference()
        for key, value, exception in (("quantity", 1.0, TypeError), ("price", "99", TypeError),
                                      ("quantity", Decimal("0"), ValueError),
                                      ("price", Decimal("-1"), ValueError),
                                      ("fee_quote", Decimal("NaN"), ValueError)):
            with self.subTest(key=key, value=value):
                arguments = dict(side="buy", quantity=Decimal("1"), price=Decimal("99"), reference=reference)
                arguments[key] = value
                with self.assertRaises(exception):
                    compute_markout(**arguments)
        with self.assertRaises(ValueError):
            compute_markout(side="hold", quantity=Decimal("1"), price=Decimal("99"), reference=reference)
