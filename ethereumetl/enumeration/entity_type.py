class EntityType:
    BLOCK = 'block'
    TRANSACTION = 'transaction'
    RECEIPT = 'receipt'
    LOG = 'log'
    TOKEN_TRANSFER = 'token_transfer'
    TRACE = 'trace'
    CONTRACT = 'contract'
    TOKEN = 'token'
    ALFRED_FOLLOW_UNFOLLOW_LOGS = 'alfred_follow_unfollow_logs'
    GMX_EXECUTE_LIMIT_ORDERS_LOGS = 'gmx_execute_limit_orders_logs'
    GMX_EXECUTE_MARKET_ORDERS_LOGS = 'gmx_execute_market_orders_logs'

    ALL_FOR_STREAMING = [BLOCK, TRANSACTION, LOG,
                         TOKEN_TRANSFER, TRACE, CONTRACT, TOKEN, ALFRED_FOLLOW_UNFOLLOW_LOGS, GMX_EXECUTE_LIMIT_ORDERS_LOGS, GMX_EXECUTE_MARKET_ORDERS_LOGS]
    ALL_FOR_INFURA = [BLOCK, TRANSACTION, LOG,
                      TOKEN_TRANSFER, ALFRED_FOLLOW_UNFOLLOW_LOGS, GMX_EXECUTE_LIMIT_ORDERS_LOGS, GMX_EXECUTE_MARKET_ORDERS_LOGS]
