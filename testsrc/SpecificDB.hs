module SpecificDB where
import Database.HDBC
import Database.HDBC.MySQL
import Test.HUnit

connectDB = 
    handleSqlError (connectMySQL "127.0.0.1" "root" "" "test" 3306 "")
