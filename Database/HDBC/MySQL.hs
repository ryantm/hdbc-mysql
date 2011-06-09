{- |

Module      : Database.HDBC.MySQL
Copyright   : Copyright (c) 2009-2010 Chris Waterson
License     : LGPL
Maintainer  : bos@mailrank.com
Stability   : experimental
Portability : GHC

This module provides a MySQL driver for the HDBC database interface.
To use it, invoke the 'connectMySQL' method to create an
@Database.HDBC.IConnection@ that you can use to interact with a MySQL
database.  Use the 'defaultMySQLConnectInfo', overriding the default
values as necessary.

@
import Control.Monad
import Database.HDBC
import Database.HDBC.MySQL

main = do
  rows <- 'withRTSSignalsBlocked' $ do
    conn <- 'connectMySQL' 'defaultMySQLConnectInfo' {
              'mysqlHost'     = \"db1.example.com\",
              'mysqlUser'     = \"scott\",
              'mysqlPassword' = \"tiger\"
            }
    'quickQuery'' conn \"SELECT 1 + 1\" []
  forM_ rows $ \\row -> putStrLn $ show row
@

There are some important caveats to note about this driver.

* RTS signals.  If you are writing an application that links against GHC's
  threaded runtime (as most server-side applications do), you must use
  'withRTSSignalsBlocked' to defend the @mysqlclient@ library against the
  signals the RTS uses, or you may experience crashes.

* Transaction support.  The MySQL server supports a
  variety of backend \"engines\", only some of which support
  transactional access (e.g., InnoDB).  This driver will report that the
  database supports transactions.  Should you decide to make use of the
  transactional support in the HDBC API,
  /it is up to you to make sure that you use a MySQL engine that supports transactions/.

* Dates and times.  MySQL does not store time zone
  information in @DATETIME@ or @TIMESTAMP@ columns: instead, it assumes
  that all dates are stored in the \"server's time zone\".  At some
  point in the future, this driver may query for the server's time zone
  and apply appropriate time zone conversion to these datatypes. For
  now, it simply treats all times as UTC; i.e., it assumes the server's
  time zone is UTC.

-}
module Database.HDBC.MySQL
    (
      MySQLConnectInfo(..)
    , Connection
    , connectMySQL
    , defaultMySQLConnectInfo
    , withRTSSignalsBlocked
    ) where

import Database.HDBC.MySQL.Connection
import Database.HDBC.MySQL.RTS
