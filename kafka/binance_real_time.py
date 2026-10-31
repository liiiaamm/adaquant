# -*- coding: utf-8 -*-
from binance import Client
from binance import ThreadedWebsocketManager

def handle_kline(msg):
    k = msg['k']
    print(
        f"Intervalo: {k['i']} | "
        f"Cierre: {k['c']} | "
        f"Volumen: {k['v']}"
    )

twm = ThreadedWebsocketManager()
twm.start()

twm.start_kline_socket(
    symbol='BTCUSDT',
    interval=Client.KLINE_INTERVAL_1MINUTE,
    callback=handle_kline
)

input("Pulsa ENTER para salir\n")
twm.stop()