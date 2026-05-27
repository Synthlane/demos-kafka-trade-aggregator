import faust


class Trade(faust.Record):
    symbol: str
    price: float
    qty: float
    time: int
    buyer_maker: bool
