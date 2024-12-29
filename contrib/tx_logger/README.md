# tx_logger Extension

This extension logs transaction start, commit, and rollback events into a log table.

## Installation

To install the extension, run the following commands in the extension directory:

```sh
make
sudo make install

Then, in your PostgreSQL database, run:
CREATE EXTENSION tx_logger;



To log transaction events, call the appropriate functions at the beginning and end of your transactions:
BEGIN;
SELECT log_transaction_start();


-- Perform your transaction operations here


-- To commit the transaction and log the commit event
SELECT log_transaction_commit();

-- Alternatively, to rollback the transaction and log the rollback event

-- SELECT log_transaction_rollback();


To uninstall the extension, run:
DROP EXTENSION tx_logger;



###  编译和安装扩展

在扩展目录下运行以下命令来编译和安装扩展：

```sh
make
sudo make install



CREATE EXTENSION tx_logger;



###  测试
BEGIN;
SELECT log_transaction_start();


-- Perform some operations here


-- Commit the transaction and log the commit event
SELECT log_transaction_commit();


-- Check the log table
SELECT * FROM tx_logger;