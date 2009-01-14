module Main where

import Control.Monad
import Database.HDBC
import Database.HDBC.MySQL

main :: IO ()
main = do conn <- connectMySQL "127.0.0.1" "root" "" "test" 3306 ""

          putStrLn $ "you are connected to " ++ (hdbcDriverName conn) ++
                   " server version " ++ (dbServerVer conn) ++
                   " via client version " ++ (hdbcClientVer conn)

          tables <- getTables conn
          putStrLn $ "the tables in this database are " ++ (show tables)

          stmt <- prepare conn "CREATE TABLE foo (bar VARCHAR(20))"
          execute stmt []
--          rows <- fetchAllRows stmt
--          forM_ rows $ \row -> putStrLn $ show row
          disconnect conn
