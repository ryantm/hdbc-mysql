module SpecificDB where
import Database.HDBC
import Database.HDBC.MySQL
import Test.HUnit

connectDB = 
    handleSqlError (connectMySQL defaultMySQLConnectInfo)
