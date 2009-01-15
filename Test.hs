module Main where

import Control.Monad
import Database.HDBC
import Database.HDBC.MySQL

go :: IO ()
go = do conn <- connectMySQL defaultMySQLConnectInfo
--                { mysqlPort = 13306
--                , mysqlDatabase = "event"
--                }

        putStrLn $ "you are connected to " ++ (hdbcDriverName conn) ++
                     " server version " ++ (dbServerVer conn) ++
                     " via client version " ++ (hdbcClientVer conn)

        tables <- getTables conn
        putStrLn $ "the tables in this database are " ++ (show tables)

        stmt <- prepare conn "SELECT NOW()"
        execute stmt []
        rows <- fetchAllRows stmt
        forM_ rows $ \row -> putStrLn $ show row
        disconnect conn


main :: IO ()
main = handleSqlError go
