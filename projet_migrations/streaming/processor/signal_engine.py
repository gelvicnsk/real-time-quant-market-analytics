import math
from collections import deque
import numpy as np

class SignalEngine:
    """
    Logistic regression ONLINE simplifiée
    Inspirée des modèles de market microstructure
    """

    def __init__(self):
        self.prices = {}
        self.returns = {}

    def update(self, symbol: str, price: float):
        if symbol not in self.prices:
            self.prices[symbol] = deque(maxlen=50)
            self.returns[symbol] = deque(maxlen=50)

        p = self.prices[symbol]
        r = self.returns[symbol]

        if len(p) > 0:
            ret = math.log(price / p[-1])
            r.append(ret)

        p.append(price)

        if len(r) < 10:
            return 0, 0.5, 0, 0

        momentum = np.mean(r[-5:])
        volatility = np.std(r[-20:]) + 1e-6

        # Logistic model (coefficients réalistes)
        score = 1 / (1 + math.exp(-(12*momentum - 4*volatility)))

        if score > 0.55:
            signal = 1
        elif score < 0.45:
            signal = -1
        else:
            signal = 0

        return signal, score, momentum, volatility
