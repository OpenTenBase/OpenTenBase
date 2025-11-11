#ifndef TX_LOGGER_H
#define TX_LOGGER_H

extern Datum log_transaction_start(PG_FUNCTION_ARGS);
extern Datum log_transaction_commit(PG_FUNCTION_ARGS);
extern Datum log_transaction_rollback(PG_FUNCTION_ARGS);

#endif /* TX_LOGGER_H */