from decimal import Decimal
import unittest

from trading_bot.precision import ceil_to_step, decimal_to_api, floor_to_step


class PrecisionTests(unittest.TestCase):
    def test_floor_to_step(self):
        self.assertEqual(floor_to_step(Decimal("1.239"), Decimal("0.01")), Decimal("1.23"))

    def test_ceil_to_step(self):
        self.assertEqual(ceil_to_step(Decimal("1.231"), Decimal("0.01")), Decimal("1.24"))

    def test_decimal_to_api(self):
        self.assertEqual(decimal_to_api(Decimal("1.2300")), "1.23")
        self.assertEqual(decimal_to_api(Decimal("2.000")), "2")


if __name__ == "__main__":
    unittest.main()

