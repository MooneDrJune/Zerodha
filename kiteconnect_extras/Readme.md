# Features of `kiteconnect_extras` / KiteConnect Extras On Steriod Version
- IPC Enabled `KiteConnect` aka `KiteExt`
- IPC Enabled `KiteTicker` aka `KiteExtTicker`


    ## Additional Features of the new `KiteExt` on Steriods.
    1.  Offers Feature to be used in other brokers `Python SDk` for obtaining `historical_data` 
        from existing running master kite `KiteExt` instance on a different process. 1000's of 
        seperate processes belonging to different/same sdk can get the historical data without any issue.
    
    2.  Offers function of fetching multiple historical data at once by using `multiple_historical_data`,
        If you ask it fetch 100 instruments data, it will fetch and return you automatically handling api
        rate limits issue etc, and `12-15 req/resp per second` is easily provided with this, instead of 
        `3/sec` with original method.
    
    3.  Offers flag to have `tick-data` from `KiteExtTicker` inside the `KiteExt` instance itself, so all 
        of the kite instance can have access to tick data automatically at `kite.ticks` as a
        `Dict[instrumen_token, latest_tick_data]`. Moreover, you can dynamically, 
        `subscribe/unsubscribe/set_mode` by `kite.subscribe/kite.unsubscribe/kite.set_mode`. So if you have 
        1000's of different strategy running into 1000's of seperate process, all can have `tick_data` / 
        `order_update` data simultaneously, as well as having the ability to dynamically, `subscribe / 
        unsubscribe / set_mode`. Isn't it magic.
    
    4.  Offers Feature to be instantiated without any login for getting `tick_data` / `order_update` / 
        `historical_data` on 1000's of different processes. 
        (one master `KiteExt` instance shall be running on a diferent process)
    
    
    ## Additional Features of the new `KiteExtTicker` on Steriods.
    1.  Offers Feature to be used in other brokers `Python SDk` for obtaining `tick_data` from existing
        running master kws `KiteExtTicker` instance on a different process. 1000's of seperate processes 
        belonging to different/same sdk can get the `tick_data` without any issue, as well as will be 
        having the ability to dynamically, `subscribe/unsubscribe/set_mode`. Isn't it magic.
    
    2.  Offers Feature to be instantiated *without any login* for getting `tick_data` / `order_update` on 
        1000's of different processes. 
        (one master `KiteExtTicker` instance shall be running on a diferent process)
