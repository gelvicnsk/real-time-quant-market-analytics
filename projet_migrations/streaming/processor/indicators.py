import math
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Tuple, Optional

@dataclass
class IndicatorState:
    prices: Deque[float]

class IndicatorEngine:
    def __init__(self, sma_window: int = 5):
        self.sma_window = sma_window
        self.state: Dict[str, IndicatorState] = {}

    def update(self, symbol: str, close: float) -> Tuple[float, float]:
        """
        Returns (return_log, sma)
        """
        st = self.state.get(symbol)
        if st is None:
            st = IndicatorState(prices=deque(maxlen=self.sma_window))
            self.state[symbol] = st

        prices = st.prices
        prev: Optional[float] = prices[-1] if len(prices) > 0 else None
        prices.append(close)

        ret = 0.0
        if prev is not None and prev > 0 and close > 0:
            ret = math.log(close / prev)

        sma = sum(prices) / len(prices)
        return ret, sma
