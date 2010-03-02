{- |

This module provides a MySQL driver for the HDBC database interface.
To use it, invoke the 'connectMySQL' method to create an
@Database.HDBC.IConnection@ that you can use to interact with a MySQL
database.  Use the 'defaultMySQLConnectInfo', overriding the default
values as necessary.

> import Control.Monad
> import Database.HDBC
> import Database.HDBC.MySQL
> main = do conn <- connectMySQL defaultMySQLConnectInfo {
>                        mysqlHost     = "db1.example.com",
>                        mysqlUser     = "scott",
>                        mysqlPassword = "tiger"
>                     }
>
>           rows <- quickQuery' conn "SELECT 1 + 1" []
>           forM_ rows $ \row -> putStrLn $ show row

There are some important caveats to note about this driver.

The first relates to transaction support.  The MySQL server supports a
variety of backend \"engines\", only some of which support
transactional access (e.g., InnoDB).  This driver will report that the
database supports transactions.  Should you decide to make use of the
transactional support in the HDBC API,
/it is up to you to make sure that you use a MySQL engine that supports transactions/.

The next relates to dates and times.  MySQL does not store time zone
information in @DATETIME@ or @TIMESTAMP@ columns: instead, it assumes
that all dates are stored in the \"server's time zone\".  At some
point in the future, this driver may query for the server's time zone
and apply appropriate time zone conversion to these datatypes. For
now, it simply treats all times as UTC; i.e., it assumes the server's
time zone is UTC.

-}
module Database.HDBC.MySQL
    (connectMySQL, MySQLConnectInfo(..), defaultMySQLConnectInfo, Connection)
where
import Database.HDBC.MySQL.Connection
