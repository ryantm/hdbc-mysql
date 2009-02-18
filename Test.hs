module Main where

import System.IO.Unsafe
import Control.Monad
import Database.HDBC
import Database.HDBC.MySQL

go :: IO ()
go = do conn <- connectMySQL defaultMySQLConnectInfo
                { mysqlHost = "madchenwagen"
                }

        rows <- quickQuery' conn "SELECT * FROM album" []
        forM_ rows $ \row -> putStrLn $ show row

main :: IO ()
main = handleSqlError (replicateM_ 1000 go)
