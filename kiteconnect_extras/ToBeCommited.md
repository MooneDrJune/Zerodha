# Next Commit Will Bring These Functions:

 ```python
multiple_login_with_credntials(
    List[Tuple[User_Id, PassWord, TwoFa_Secrets]*N]]
)
```

### Provided Single / Multiple Users Logged In With Credentials Instead Of Enctoken
```python
Auto_Relogin_On_session_Expire: bool = True / False  # Default False
```

### Do Multiple Things On Multiple Account At Once With One Call

```python
place_order_multiple(
    orders: List[Dict[OrderParamsKey, OrderParamsValue]] = [{}],
    AllSellLegsFirst:bool = True/False,   # Default True
    AllBuyLegsFirst:bool = True/False,   # Default False
    InSequenceAsPerOrderList:bool = True/False,   # Default False
    ToSpecificUserIdAccount: Optional[str] = "ABCD1234"  # Default None
    ToAllLoggedInUserAccounts: bool = True/False,   # Default False
)
```
And All Other Functions `margins, profile, modify_order, cancel_order, exit_order, orders, order_history, trades, order_trades, positions, holdings, convert_position, quote, ohlc, ltp, historical_data, trigger_range, get_gtt/s, place_gtt, modify_gtt, delete_gtt, order_margins, basket_order_margins, get_virtual_contract_note etc..`
With these Same Parameter As Showcased Above In `place_order_multiple` To Support Multiple Requests At Once With The Facility To Send To AllUserLoggedInAccounts / ToSpecificUserIdAccount

 
### Plus Some Enchantress Magic... 🪄
