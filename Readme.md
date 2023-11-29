# Minimalistic pump and dump client

This client uses ccxt so it can easy be extended to handle other exchanges but currently is only using Kucoin.
Before running create a `.env` file with the following keys.

    API_KEY=
    API_SECRET=
    PASSWORD=
    BASE_CURRENCY=
    AUTO_CLOSE=True
    CLOSING_DRAW_DOWN=98

Base currency is the coin which you want to use to fund your trades.

The `requirements.txt` file contains all the used packages at the moment
so use it ot create your python environment.

Then just run the client in your preferred way.

The strategy is simple.
1. Configure the session
   2. Asks for Auto closing mode (Yes or no)
   3. Eventually for draw-down tolerance (default 98%)
2. Waits for the name of the coin
3. Creates Market order
4. Either auto-closes when draw-down tolerance is reached
5. Or upon CTRL-C asks if you want to close the position

All executed orders and price ticks are persisted in a json file.
The position and price evolution are stored in a csv file.

# WARNING: Use at your own risk!!
The current behaviour is to use all your account funds in the configured
Base currency, be sure to adjust the script or configure the API to use the correct account.

Use the source, this readme is not up to date, but will be at one stage :)