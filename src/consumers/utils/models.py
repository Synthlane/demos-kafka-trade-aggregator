import faust

class Trade(faust.Record, serializer='json'):
    symbol: str
    qty: float
    price: float
    time: int  # epoch ms

    @property
    def event_ts(self) -> float:
        return self.time / 1000.0