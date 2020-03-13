{-# LANGUAGE EmptyDataDecls, ForeignFunctionInterface, ScopedTypeVariables, CPP #-}

module Database.HDBC.MySQL.Connection
    (
      connectMySQL
    , MySQLConnectInfo(..)
    , defaultMySQLConnectInfo
    , Connection
    ) where

import Control.Exception
import Control.Monad
import Foreign
import Foreign.C
import qualified Foreign.Concurrent
import qualified Data.ByteString as B
import Data.ByteString.UTF8 (fromString, toString)
import Data.List (isPrefixOf)
import Data.Time
import Data.Time.Clock.POSIX

import qualified Database.HDBC.Types as Types
import Database.HDBC.ColTypes as ColTypes
import Database.HDBC (throwSqlError)

#if DEBUG
import Debug.Trace
#endif

#include <mysql.h>

{- | Connection information to use with connectMySQL.

     You must either supply a host and port, or the full path to a
     Unix socket.

-}
data MySQLConnectInfo = MySQLConnectInfo
    { -- | The server's hostname; e.g., @\"db1.example.com\"@
      mysqlHost       :: String
      -- | The MySQL username to use for login; e.g., @\"scott\"@
    , mysqlUser       :: String
      -- | The MySQL password to use for login; e.g., @\"tiger\"@
    , mysqlPassword   :: String
      -- | the \"default\" database name; e.g., @\"emp\"@
    , mysqlDatabase   :: String
      -- | The port on which to connect to the server; e.g., @3306@
    , mysqlPort       :: Int
      -- | The absolute path of the server's Unix socket; e.g., @\"\/var\/lib\/mysql.sock\"@
    , mysqlUnixSocket :: String
      -- | The group name in my.cnf from which it reads options; e.g., @\"test\"@
    , mysqlGroup      :: Maybe String
    }

{- | Typical connection information, meant to be overridden partially,
     for example:

     > connectMySQL defaultMySQLConnectInfo { mysqlHost = "db1" }

     In particular, the default values are @\"127.0.0.1\"@ as the
     host, @3306@ as the port, @\"root\"@ as the user, no password,
     and @\"test\"@ as the default database.

-}
defaultMySQLConnectInfo :: MySQLConnectInfo
defaultMySQLConnectInfo = MySQLConnectInfo "127.0.0.1" "root" "" "test" 3306 "" Nothing

data Connection = Connection
    { disconnect :: IO ()
    , commit :: IO ()
    , rollback :: IO ()
    , run :: String -> [Types.SqlValue] -> IO Integer
    , runRaw :: String -> IO ()
    , prepare :: String -> IO Types.Statement
    , clone :: IO Connection
    , hdbcDriverName :: String
    , hdbcClientVer :: String
    , proxiedClientName :: String
    , proxiedClientVer :: String
    , dbServerVer :: String
    , dbTransactionSupport :: Bool
    , getTables :: IO [String]
    , describeTable :: String -> IO [(String, ColTypes.SqlColDesc)]
    }

instance Types.IConnection Connection where
  disconnect           = disconnect
  commit               = commit
  rollback             = rollback
  run                  = run
  runRaw               = runRaw
  prepare              = prepare
  clone                = clone
  hdbcDriverName       = hdbcDriverName
  hdbcClientVer        = hdbcClientVer
  proxiedClientName    = proxiedClientName
  proxiedClientVer     = proxiedClientVer
  dbServerVer          = dbServerVer
  dbTransactionSupport = dbTransactionSupport
  getTables            = getTables
  describeTable        = describeTable

-- The "real" connection to the MySQL server.  Wraps mysql.h's MYSQL
-- struct.  We don't ever need to look inside it.
data MYSQL

{- | Connects to a MySQL database using the specified
     connection information. -}
connectMySQL :: MySQLConnectInfo -> IO Connection
connectMySQL info = do
  mysql_ <- mysql_init nullPtr
  when (mysql_ == nullPtr) (error "mysql_init failed")
  case mysqlGroup info of
    Just group -> withCString group $ \group_ -> do
                      _ <- mysql_options mysql_ #{const MYSQL_READ_DEFAULT_GROUP} (castPtr group_)
                      return ()
    Nothing -> return ()
  mysql <- withCString (mysqlHost info) $ \host_ ->
      withCString (mysqlUser info) $ \user_ ->
          withCString (mysqlPassword info) $ \passwd_ ->
              withCString (mysqlDatabase info) $ \db_ ->
                  withCString (mysqlUnixSocket info) $ \unixSocket_ ->
                      do rv <- mysql_real_connect mysql_ host_ user_ passwd_ db_
                                                  (fromIntegral $ mysqlPort info)
                                                  unixSocket_ 0
                         when (rv == nullPtr) (connectionError mysql_)
                         wrap mysql_
  withCString "utf8mb4" $ \csname_ -> do
      rv0 <- mysql_set_character_set mysql_ csname_
      when (rv0 /= 0) (error "mysql_set_character_set failed")
  return mysql
    where
      -- Returns the HDBC wrapper for the native MySQL connection
      -- object.
      wrap :: Ptr MYSQL -> IO Connection
      wrap mysql_ = do
        clientver <- peekCString =<< mysql_get_client_info
        serverver <- peekCString =<< mysql_get_server_info mysql_
        protover <- mysql_get_proto_info mysql_

        -- HDBC assumes that there is no such thing as auto-commit.
        -- So we'll turn it off here and start our first transaction.
        _ <- mysql_autocommit mysql_ 0

        mysql__ <- newForeignPtr mysql_close mysql_
        doStartTransaction mysql__

        return $ Connection
                   { disconnect           = finalizeForeignPtr mysql__
                   , commit               = doCommit mysql__ >> doStartTransaction mysql__
                   , rollback             = doRollback mysql__ >> doStartTransaction mysql__
                   , run                  = doRun mysql__
                   , runRaw               = flip doQuery mysql__
                   , prepare              = newStatement mysql__
                   , clone                = connectMySQL info
                   , hdbcDriverName       = "mysql"
                   , hdbcClientVer        = clientver
                   , proxiedClientName    = "mysql"
                   , proxiedClientVer     = show protover
                   , dbServerVer          = serverver
                   , dbTransactionSupport = True
                   , getTables            = doGetTables mysql__
                   , describeTable        = doDescribeTable mysql__
                   }

-- A MySQL statement: wraps mysql.h's MYSQL_STMT struct.
data MYSQL_STMT

-- A MySQL result: wraps mysql.h's MYSQL_RES struct.
data MYSQL_RES

-- A MySQL field: wraps mysql.h's MYSQL_FIELD struct.  We do actually
-- have to spelunk this structure, so it's a Storable instance.
data MYSQL_FIELD = MYSQL_FIELD
    { fieldName      :: String
    , fieldLength    :: CULong
    , fieldMaxLength :: CULong
    , fieldType      :: CInt
    , fieldDecimals  :: CUInt
    , fieldFlags     :: CUInt
    }

instance Storable MYSQL_FIELD where
    sizeOf _     = #const sizeof(MYSQL_FIELD)
    alignment _  = alignment (undefined :: CInt)

    peek p = do
      fname   <- peekCString =<< (#peek MYSQL_FIELD, name) p
      flength <- (#peek MYSQL_FIELD, length) p
      fmaxlen <- (#peek MYSQL_FIELD, max_length) p
      ftype   <- (#peek MYSQL_FIELD, type) p
      fdec    <- (#peek MYSQL_FIELD, decimals) p
      fflags  <- (#peek MYSQL_FIELD, flags) p
      return $ MYSQL_FIELD
                 { fieldName      = fname
                 , fieldLength    = flength
                 , fieldMaxLength = fmaxlen
                 , fieldType      = ftype
                 , fieldDecimals  = fdec
                 , fieldFlags     = fflags
                 }

    poke _ _ = error "MYSQL_FIELD: poke"

-- A MySQL binding to a query parameter or result.  This wraps
-- mysql.h's MYSQL_BIND struct, and it's also Storable -- in this
-- case, so that we can create them.
data MYSQL_BIND = MYSQL_BIND
    { bindLength       :: Ptr CULong
    , bindIsNull       :: Ptr CChar
    , bindBuffer       :: Ptr ()
    , bindError        :: Ptr CChar
    , bindBufferType   :: CInt
    , bindBufferLength :: CULong
    , bindIsUnsigned   :: CChar
    }

data Signedness = Signed | Unsigned deriving (Eq)

instance Storable MYSQL_BIND where
    sizeOf _      = #const sizeof(MYSQL_BIND)
    alignment _   = alignment (undefined :: CInt)

    peek _ = error "MYSQL_BIND: peek"

    poke p (MYSQL_BIND len_ isNull_ buf_ err_ buftyp buflen unsigned) = do
        memset (castPtr p) 0 #{const sizeof(MYSQL_BIND)}
        (#poke MYSQL_BIND, length)        p len_
        (#poke MYSQL_BIND, is_null)       p isNull_
        (#poke MYSQL_BIND, buffer)        p buf_
        (#poke MYSQL_BIND, error)         p err_
        (#poke MYSQL_BIND, buffer_type)   p buftyp
        (#poke MYSQL_BIND, buffer_length) p buflen
        (#poke MYSQL_BIND, is_unsigned)   p unsigned

data MYSQL_TIME = MYSQL_TIME
    { timeYear       :: CUInt
    , timeMonth      :: CUInt
    , timeDay        :: CUInt
    , timeHour       :: CUInt
    , timeMinute     :: CUInt
    , timeSecond     :: CUInt
    }

instance Storable MYSQL_TIME where
    sizeOf _      = #const sizeof(MYSQL_TIME)
    alignment _   = alignment (undefined :: CInt)

    peek p = do
      year    <- (#peek MYSQL_TIME, year) p
      month   <- (#peek MYSQL_TIME, month) p
      day     <- (#peek MYSQL_TIME, day) p
      hour    <- (#peek MYSQL_TIME, hour) p
      minute  <- (#peek MYSQL_TIME, minute) p
      second  <- (#peek MYSQL_TIME, second) p
      return (MYSQL_TIME year month day hour minute second)

    poke p t = do
      memset (castPtr p) 0 #{const sizeof(MYSQL_TIME)}
      (#poke MYSQL_TIME, year)   p (timeYear t)
      (#poke MYSQL_TIME, month)  p (timeMonth t)
      (#poke MYSQL_TIME, day)    p (timeDay t)
      (#poke MYSQL_TIME, hour)   p (timeHour t)
      (#poke MYSQL_TIME, minute) p (timeMinute t)
      (#poke MYSQL_TIME, second) p (timeSecond t)

-- Prepares a new Statement for execution.
newStatement :: ForeignPtr MYSQL -> String -> IO Types.Statement
newStatement mysql__ query = withForeignPtr mysql__ $ \mysql_ -> do
  stmt_ <- mysql_stmt_init mysql_
  when (stmt_ == nullPtr) (connectionError mysql_)

  -- If an error occurs below, we'll lose the reference to the foreign
  -- pointer and run the finalizer.
  stmt__ <- Foreign.Concurrent.newForeignPtr stmt_ (mysql_stmt_close stmt_)

  withCStringLen query $ \(query_, len) -> do
      rv <- mysql_stmt_prepare stmt_ query_ (fromIntegral len)
      when (rv /= 0) (statementError stmt_)

  -- Collect the result fields of the statement; this will simply be
  -- the empty list if we're doing something that doesn't generate
  -- results.
  fields <- fieldsOf stmt_

  -- Create MYSQL_BIND structures for each field and point the the
  -- statement at those buffers.  Again, if there are no fields,
  -- this'll be a no-op.
  results <- mapM resultOfField fields
  when (not $ null results)
       (withArray results $ \bind_ -> do
          rv' <- mysql_stmt_bind_result stmt_ bind_
          when (rv' /= 0) (statementError stmt_))

  Foreign.Concurrent.addForeignPtrFinalizer stmt__ (freeBinds results)

  -- We pass the connection ForeignPtr down to execute and fetchRow as
  -- a silly way to keep a reference to it alive so long as the
  -- statement is around.
  return $ Types.Statement
             { Types.execute        = execute mysql__ stmt__
             , Types.executeRaw     = executeRaw mysql__ stmt__
             , Types.executeMany    = mapM_ $ execute mysql__ stmt__
             , Types.finish         = finalizeForeignPtr stmt__
             , Types.fetchRow       = fetchRow mysql__ stmt__ results
             , Types.originalQuery  = query
             , Types.getColumnNames = return $ map fieldName fields
             , Types.describeResult = return $ map sqlColDescOf fields
             }

-- Release the storage allocated for each bind.
freeBinds :: [MYSQL_BIND] -> IO ()
freeBinds binds = do
  mapM_ freeOneBind binds
    where freeOneBind bind = do
            free $ bindLength bind
            free $ bindIsNull bind
            free $ bindBuffer bind
            free $ bindError bind

-- Returns the list of fields from a prepared statement.
fieldsOf :: Ptr MYSQL_STMT -> IO [MYSQL_FIELD]
fieldsOf stmt_ = bracket acquire release fieldsOf'
    where acquire                          = mysql_stmt_result_metadata stmt_
          release res_ | res_ == nullPtr   = return ()
                       | otherwise         = mysql_free_result res_
          fieldsOf' res_ | res_ == nullPtr = return []
                         | otherwise       = fieldsOfResult res_

-- Builds the list of fields from the result set metadata: this is
-- just a helper function for fieldOf, above.
fieldsOfResult :: Ptr MYSQL_RES -> IO [MYSQL_FIELD]
fieldsOfResult res_ = do
  field_ <- mysql_fetch_field res_
  if (field_ == nullPtr)
    then return []
    else liftM2 (:) (peek field_) (fieldsOfResult res_)

-- Executes a statement with the specified binding parameters.
execute :: ForeignPtr MYSQL -> ForeignPtr MYSQL_STMT -> [Types.SqlValue] -> IO Integer
execute mysql__ stmt__ params =
    withForeignPtr mysql__ $ \_ ->
        withForeignPtr stmt__ $ \stmt_ -> do
          binds <- bindParams stmt_ params
          rv <- mysql_stmt_execute stmt_
          freeBinds binds
          when (rv /= 0) (statementError stmt_)
          nrows <- mysql_stmt_affected_rows stmt_

          -- mysql_stmt_affected_rows returns -1 when called on a
          -- SELECT statement; the HDBC API expects zero to be
          -- returned in this case.
          return $ fromIntegral (if nrows == (-1 :: CULLong) then 0 else nrows)

-- Binds placeholder parameters to values.
bindParams :: Ptr MYSQL_STMT -> [Types.SqlValue] -> IO [MYSQL_BIND]
bindParams stmt_ params = do
  param_count <- mysql_stmt_param_count stmt_
  let nparams = fromIntegral param_count

  -- XXX i'm not sure if it makes more sense to keep this paranoia, or
  -- to simply remove it.  The code that immediately follows pads
  -- extra bind parameters with nulls.
  when (nparams /= length params)
           (error "the number of parameter placeholders in the prepared SQL is different than the number of parameters provided")

  let params' = take nparams $ params ++ repeat Types.SqlNull
  binds <- mapM bindOfSqlValue params'
  withArray binds $ \bind_ -> do
      rv <- mysql_stmt_bind_param stmt_ bind_
      when (rv /= 0) (statementError stmt_)

  return binds

-- Given a SqlValue, return a MYSQL_BIND structure that we can use to
-- pass its value.
bindOfSqlValue :: Types.SqlValue -> IO MYSQL_BIND

bindOfSqlValue Types.SqlNull = do
  isNull_ <- new (1 :: CChar)
  return $ MYSQL_BIND
             { bindLength       = nullPtr
             , bindIsNull       = isNull_
             , bindBuffer       = nullPtr
             , bindError        = nullPtr
             , bindBufferType   = #{const MYSQL_TYPE_NULL}
             , bindBufferLength = 0
             , bindIsUnsigned   = 0
             }

bindOfSqlValue (Types.SqlString s) = do
  bindOfSqlValue (Types.SqlByteString $ fromString s)

bindOfSqlValue (Types.SqlByteString s) = do
  B.useAsCString s $ \c_ -> do
    let len = B.length s
    buf_ <- mallocBytes len
    copyBytes buf_ c_ len
    bindOfSqlValue' len buf_ #{const MYSQL_TYPE_VAR_STRING} Signed

bindOfSqlValue (Types.SqlInteger n) = do
  buf_ <- new (fromIntegral n :: CLLong)
  bindOfSqlValue' (8::Int) buf_ #{const MYSQL_TYPE_LONGLONG} Signed

bindOfSqlValue (Types.SqlBool b) = do
  buf_ <- new (if b then 1 else 0 :: CChar)
  bindOfSqlValue' (1::Int) buf_ #{const MYSQL_TYPE_TINY} Signed

bindOfSqlValue (Types.SqlChar c) = do
  buf_ <- new c
  bindOfSqlValue' (1::Int) buf_ #{const MYSQL_TYPE_TINY} Signed

bindOfSqlValue (Types.SqlDouble d) = do
  buf_ <- new (realToFrac d :: CDouble)
  bindOfSqlValue' (8::Int) buf_ #{const MYSQL_TYPE_DOUBLE} Signed

bindOfSqlValue (Types.SqlInt32 n) = do
  buf_ <- new n
  bindOfSqlValue' (4::Int) buf_ #{const MYSQL_TYPE_LONG} Signed

bindOfSqlValue (Types.SqlInt64 n) = do
  buf_ <- new n
  bindOfSqlValue' (8::Int) buf_ #{const MYSQL_TYPE_LONGLONG} Signed

bindOfSqlValue (Types.SqlRational n) = do
  buf_ <- new (realToFrac n :: CDouble)
  bindOfSqlValue' (8::Int) buf_ #{const MYSQL_TYPE_DOUBLE} Signed

bindOfSqlValue (Types.SqlWord32 n) = do
  buf_ <- new n
  bindOfSqlValue' (4::Int) buf_ #{const MYSQL_TYPE_LONG} Unsigned

bindOfSqlValue (Types.SqlWord64 n) = do
  buf_ <- new n
  bindOfSqlValue' (8::Int) buf_ #{const MYSQL_TYPE_LONGLONG} Unsigned

bindOfSqlValue (Types.SqlEpochTime epoch) =
  bindOfSqlValue (Types.SqlUTCTime t)
    where t = posixSecondsToUTCTime (fromIntegral epoch)
                                            
bindOfSqlValue (Types.SqlUTCTime utct) = do
  let t = utcToMysqlTime utct
  buf_ <- new t
  bindOfSqlValue' (#{const sizeof(MYSQL_TIME)}::Int) buf_ #{const MYSQL_TYPE_DATETIME} Signed
      where utcToMysqlTime :: UTCTime -> MYSQL_TIME
            utcToMysqlTime (UTCTime day difftime) =
                let (y, m, d) = toGregorian day
                    t  = floor $ (realToFrac difftime :: Double)
                    h  = t `div` 3600
                    mn = t `div` 60 `mod` 60
                    s  = t `mod` 60
                in MYSQL_TIME (fromIntegral y) (fromIntegral m) (fromIntegral d) h mn s

bindOfSqlValue (Types.SqlTimeDiff n) = do
  let h  = fromIntegral $ n `div` 3600
      mn = fromIntegral $ n `div` 60 `mod` 60
      s  = fromIntegral $ n `mod` 60
      t  = MYSQL_TIME 0 0 0 h mn s
  buf_ <- new t
  bindOfSqlValue' (#{const sizeof(MYSQL_TIME)}::Int) buf_ #{const MYSQL_TYPE_TIME} Signed

bindOfSqlValue (Types.SqlLocalDate day) = do
  let (y, m, d) = toGregorian day
      t         = MYSQL_TIME (fromIntegral y) (fromIntegral m) (fromIntegral d) 0 0 0
  buf_ <- new t
  bindOfSqlValue' (#{const sizeof(MYSQL_TIME)}::Int) buf_ #{const MYSQL_TYPE_DATE} Signed

bindOfSqlValue (Types.SqlLocalTimeOfDay time) = do
  let h  = fromIntegral $ todHour time
      mn = fromIntegral $ todMin time
      s  = floor $ todSec time
      t  = MYSQL_TIME 0 0 0 h mn s
  buf_ <- new t
  bindOfSqlValue' (#{const sizeof(MYSQL_TIME)}::Int) buf_ #{const MYSQL_TYPE_TIME} Signed

bindOfSqlValue (Types.SqlZonedLocalTimeOfDay t _) =
  bindOfSqlValue $ Types.SqlLocalTimeOfDay t

bindOfSqlValue (Types.SqlLocalTime (LocalTime day time)) = do
  let (y, m, d) = toGregorian day
      h         = fromIntegral $ todHour time
      mn        = fromIntegral $ todMin time
      s         = floor $ todSec time
      t         = MYSQL_TIME (fromIntegral y) (fromIntegral m) (fromIntegral d) h mn s
  buf_ <- new t
  bindOfSqlValue' (#{const sizeof(MYSQL_TIME)}::Int) buf_ #{const MYSQL_TYPE_DATETIME} Signed

bindOfSqlValue (Types.SqlZonedTime t) =
  bindOfSqlValue $ Types.SqlLocalTime $ zonedTimeToLocalTime t

bindOfSqlValue (Types.SqlDiffTime t) =
  bindOfSqlValue $ Types.SqlPOSIXTime t

bindOfSqlValue (Types.SqlPOSIXTime t) =
  bindOfSqlValue $ Types.SqlUTCTime $ posixSecondsToUTCTime t

-- A nasty helper function that cuts down on the boilerplate a bit.
bindOfSqlValue' :: Integral a => a -> Ptr b -> CInt -> Signedness -> IO MYSQL_BIND
bindOfSqlValue' len buf_ btype signedness = do
  let buflen = fromIntegral len
  isNull_ <- new (0 :: CChar)
  len_ <- new buflen
  return $ MYSQL_BIND
             { bindLength       = len_
             , bindIsNull       = isNull_
             , bindBuffer       = castPtr buf_
             , bindError        = nullPtr
             , bindBufferType   = btype
             , bindBufferLength = buflen
             , bindIsUnsigned   = (if signedness == Unsigned then 1 else 0)
             }

executeRaw :: ForeignPtr MYSQL -> ForeignPtr MYSQL_STMT -> IO ()
executeRaw mysql__ stmt__ =
    withForeignPtr mysql__ $ \_ ->
        withForeignPtr stmt__ $ \stmt_ -> do
          rv <- mysql_stmt_execute stmt_
          when (rv /= 0) (statementError stmt_)

-- Returns an appropriate binding structure for a field.
resultOfField :: MYSQL_FIELD -> IO MYSQL_BIND
resultOfField field =
    let ftype    = fieldType field
        unsigned = (fieldFlags field .&. #{const UNSIGNED_FLAG}) /= 0
        btype    = boundType ftype (fieldDecimals field)
        size     = boundSize btype (fieldLength field) in
    do size_   <- new size
       isNull_ <- new (0 :: CChar)
       error_  <- new (0 :: CChar)
       buffer_ <- mallocBytes (fromIntegral size)
       return $ MYSQL_BIND { bindLength       = size_
                           , bindIsNull       = isNull_
                           , bindBuffer       = buffer_
                           , bindError        = error_
                           , bindBufferType   = btype
                           , bindBufferLength = size
                           , bindIsUnsigned   = if unsigned then 1 else 0
                           }

-- Returns the appropriate result type for a particular host type.
boundType :: CInt -> CUInt -> CInt
boundType #{const MYSQL_TYPE_STRING}     _ = #{const MYSQL_TYPE_VAR_STRING}
boundType #{const MYSQL_TYPE_TINY}       _ = #{const MYSQL_TYPE_LONG}
boundType #{const MYSQL_TYPE_SHORT}      _ = #{const MYSQL_TYPE_LONG}
boundType #{const MYSQL_TYPE_INT24}      _ = #{const MYSQL_TYPE_LONG}
boundType #{const MYSQL_TYPE_YEAR}       _ = #{const MYSQL_TYPE_LONG}
boundType #{const MYSQL_TYPE_ENUM}       _ = #{const MYSQL_TYPE_LONG}
boundType #{const MYSQL_TYPE_DECIMAL}    0 = #{const MYSQL_TYPE_LONGLONG}
boundType #{const MYSQL_TYPE_DECIMAL}    _ = #{const MYSQL_TYPE_DOUBLE}
boundType #{const MYSQL_TYPE_NEWDECIMAL} 0 = #{const MYSQL_TYPE_LONGLONG}
boundType #{const MYSQL_TYPE_NEWDECIMAL} _ = #{const MYSQL_TYPE_DOUBLE}
boundType #{const MYSQL_TYPE_FLOAT}      _ = #{const MYSQL_TYPE_DOUBLE}
boundType #{const MYSQL_TYPE_BLOB}       _ = #{const MYSQL_TYPE_VAR_STRING}
boundType t                              _ = t

-- Returns the amount of storage required for a particular result
-- type.
boundSize :: CInt -> CULong -> CULong
boundSize #{const MYSQL_TYPE_LONG}      _ = 4
boundSize #{const MYSQL_TYPE_DOUBLE}    _ = 8
boundSize #{const MYSQL_TYPE_DATETIME}  _ = #{const sizeof(MYSQL_TIME)}
boundSize #{const MYSQL_TYPE_TIME}      _ = #{const sizeof(MYSQL_TIME)}
boundSize #{const MYSQL_TYPE_NEWDATE}   _ = #{const sizeof(MYSQL_TIME)}
boundSize #{const MYSQL_TYPE_DATE}      _ = #{const sizeof(MYSQL_TIME)}
boundSize #{const MYSQL_TYPE_TIMESTAMP} _ = #{const sizeof(MYSQL_TIME)}
boundSize _                             n = n

-- Fetches a row from an executed statement and converts the cell
-- values into a list of SqlValue types.
fetchRow :: ForeignPtr MYSQL -> ForeignPtr MYSQL_STMT -> [MYSQL_BIND] -> IO (Maybe [Types.SqlValue])
fetchRow mysql__ stmt__ results =
  withForeignPtr mysql__ $ \_ ->
      withForeignPtr stmt__ $ \stmt_ -> do
          rv <- mysql_stmt_fetch stmt_
          case rv of
            0                             -> row
            #{const MYSQL_DATA_TRUNCATED} -> liftM Just $ mapM (uncurry $ fill stmt_) $ zip [0..] results
            #{const MYSQL_NO_DATA}        -> finalizeForeignPtr stmt__ >> return Nothing
            _                             -> statementError stmt_
    where row = liftM Just $ mapM cellValue results
          fill stmt_ column bind = do
            err <- peek $ bindError bind
            if err == 1 then do len <- peek $ bindLength bind
                                bracket (mallocBytes $ fromIntegral len) free $ \buffer_ ->
                                    do let tempBind = bind { bindBuffer = buffer_, bindBufferLength = len }
                                       rv <- with tempBind $ \bind_ -> mysql_stmt_fetch_column stmt_ bind_ column 0
                                       when (rv /= 0) (statementError stmt_)
                                       cellValue tempBind
                        else cellValue bind

-- Produces a single SqlValue cell value given the binding, handling
-- nulls appropriately.
cellValue :: MYSQL_BIND -> IO Types.SqlValue
cellValue bind = do
  isNull <- peek $ bindIsNull bind
  if isNull == 0 then cellValue' else return Types.SqlNull
      where cellValue' = do
                   len <- peek $ bindLength bind
                   let buftype  = bindBufferType bind
                       buf      = bindBuffer bind
                       unsigned = bindIsUnsigned bind == 1
                   nonNullCellValue buftype buf len unsigned

-- Produces a single SqlValue from the binding's type and buffer
-- pointer.  It assumes that the value is not null.
nonNullCellValue :: CInt -> Ptr () -> CULong -> Bool -> IO Types.SqlValue

nonNullCellValue #{const MYSQL_TYPE_LONG} p _ u = do
  n :: CInt <- peek $ castPtr p
  return $ if u then Types.SqlWord32 (fromIntegral n)
                else Types.SqlInt32 (fromIntegral n)

nonNullCellValue #{const MYSQL_TYPE_LONGLONG} p _ u = do
  n :: CLLong <- peek $ castPtr p
  return $ if u then Types.SqlWord64 (fromIntegral n)
                else Types.SqlInt64 (fromIntegral n)

nonNullCellValue #{const MYSQL_TYPE_DOUBLE} p _ _ = do
  n :: CDouble <- peek $ castPtr p
  return $ Types.SqlDouble (realToFrac n)

nonNullCellValue #{const MYSQL_TYPE_VAR_STRING} p len _ =
    B.packCStringLen ((castPtr p), fromIntegral len) >>= return . Types.SqlByteString

nonNullCellValue #{const MYSQL_TYPE_TIMESTAMP} p _ _ = do
  t :: MYSQL_TIME <- peek $ castPtr p
  let secs = (utcTimeToPOSIXSeconds . mysqlTimeToUTC) t
  return $ Types.SqlPOSIXTime secs
      where mysqlTimeToUTC :: MYSQL_TIME -> UTCTime
            mysqlTimeToUTC (MYSQL_TIME y m d h mn s) =
                -- XXX so, this is fine if the date we're getting back
                -- is UTC.  If not, well, it's wrong.
                let day = fromGregorian (fromIntegral y) (fromIntegral m) (fromIntegral d)
                    time = s + mn * 60 + h * 3600
                in UTCTime day (secondsToDiffTime $ fromIntegral time)

nonNullCellValue #{const MYSQL_TYPE_DATETIME} p _ _ = do
  (MYSQL_TIME y m d h mn s) <- peek $ castPtr p
  let date = fromGregorian (fromIntegral y) (fromIntegral m) (fromIntegral d)
      time = TimeOfDay (fromIntegral h) (fromIntegral mn) (fromIntegral s)
  return $ Types.SqlLocalTime (LocalTime date time)

nonNullCellValue #{const MYSQL_TYPE_TIME} p _ _ = do
  (MYSQL_TIME _ _ _ h mn s) <- peek $ castPtr p
  let time = TimeOfDay (fromIntegral h) (fromIntegral mn) (fromIntegral s)
  return $ Types.SqlLocalTimeOfDay time

nonNullCellValue #{const MYSQL_TYPE_DATE} p _ _ = do
  (MYSQL_TIME y m d _ _ _) <- peek $ castPtr p
  let date = fromGregorian (fromIntegral y) (fromIntegral m) (fromIntegral d)
  return $ Types.SqlLocalDate date

nonNullCellValue #{const MYSQL_TYPE_NEWDATE} p _ _ = do
  (MYSQL_TIME y m d _ _ _) <- peek $ castPtr p
  let date = fromGregorian (fromIntegral y) (fromIntegral m) (fromIntegral d)
  return $ Types.SqlLocalDate date

nonNullCellValue t _ _ _ = return $ Types.SqlString ("unknown type " ++ show t)

-- Cough up the column metadata for a field that's returned from a
-- query.
sqlColDescOf :: MYSQL_FIELD -> (String, ColTypes.SqlColDesc)
sqlColDescOf f =
    let typ      = typeIdOf (fieldType f)
        sz       = Just $ fromIntegral $ fieldLength f
        octlen   = Just $ fromIntegral $ fieldLength f
        digits   = Just $ fromIntegral $ fieldDecimals f
        nullable = Just $ (fieldFlags f .&. #{const NOT_NULL_FLAG}) == 0
    in (fieldName f, ColTypes.SqlColDesc typ sz octlen digits nullable)

-- Returns the HDBC column type appropriate for the MySQL column
-- type. (XXX as far as I can tell, I can't tell the difference
-- between a TEXT and a BLOB column, here.)
typeIdOf :: CInt -> ColTypes.SqlTypeId
typeIdOf #{const MYSQL_TYPE_DECIMAL}     = ColTypes.SqlDecimalT
typeIdOf #{const MYSQL_TYPE_TINY}        = ColTypes.SqlTinyIntT
typeIdOf #{const MYSQL_TYPE_SHORT}       = ColTypes.SqlSmallIntT
typeIdOf #{const MYSQL_TYPE_LONG}        = ColTypes.SqlIntegerT
typeIdOf #{const MYSQL_TYPE_FLOAT}       = ColTypes.SqlFloatT
typeIdOf #{const MYSQL_TYPE_DOUBLE}      = ColTypes.SqlDoubleT
typeIdOf #{const MYSQL_TYPE_NULL}        = ColTypes.SqlUnknownT "NULL"
typeIdOf #{const MYSQL_TYPE_TIMESTAMP}   = ColTypes.SqlTimestampT
typeIdOf #{const MYSQL_TYPE_LONGLONG}    = ColTypes.SqlNumericT
typeIdOf #{const MYSQL_TYPE_INT24}       = ColTypes.SqlIntegerT
typeIdOf #{const MYSQL_TYPE_DATE}        = ColTypes.SqlDateT
typeIdOf #{const MYSQL_TYPE_TIME}        = ColTypes.SqlTimeT
typeIdOf #{const MYSQL_TYPE_DATETIME}    = ColTypes.SqlTimestampT
typeIdOf #{const MYSQL_TYPE_YEAR}        = ColTypes.SqlNumericT
typeIdOf #{const MYSQL_TYPE_NEWDATE}     = ColTypes.SqlDateT
typeIdOf #{const MYSQL_TYPE_VARCHAR}     = ColTypes.SqlVarCharT
typeIdOf #{const MYSQL_TYPE_BIT}         = ColTypes.SqlBitT
typeIdOf #{const MYSQL_TYPE_NEWDECIMAL}  = ColTypes.SqlDecimalT
typeIdOf #{const MYSQL_TYPE_ENUM}        = ColTypes.SqlUnknownT "ENUM"
typeIdOf #{const MYSQL_TYPE_SET}         = ColTypes.SqlUnknownT "SET"
typeIdOf #{const MYSQL_TYPE_TINY_BLOB}   = ColTypes.SqlBinaryT
typeIdOf #{const MYSQL_TYPE_MEDIUM_BLOB} = ColTypes.SqlBinaryT
typeIdOf #{const MYSQL_TYPE_LONG_BLOB}   = ColTypes.SqlBinaryT
typeIdOf #{const MYSQL_TYPE_BLOB}        = ColTypes.SqlBinaryT
typeIdOf #{const MYSQL_TYPE_VAR_STRING}  = ColTypes.SqlVarCharT
typeIdOf #{const MYSQL_TYPE_STRING}      = ColTypes.SqlCharT
typeIdOf #{const MYSQL_TYPE_GEOMETRY}    = ColTypes.SqlUnknownT "GEOMETRY"
typeIdOf n                               = ColTypes.SqlUnknownT ("unknown type " ++ show n)

-- Run a query and discard the results, if any.
doRun :: ForeignPtr MYSQL -> String -> [Types.SqlValue] -> IO Integer
doRun mysql__ query params = do
  stmt <- newStatement mysql__ query
  rv <- Types.execute stmt params
  Types.finish stmt
  return rv

-- Issue a query "old school", without using the prepared statement
-- API.  We use this internally to send the transaction-related
-- statements, because -- it turns out -- we have to!
doQuery :: String -> ForeignPtr MYSQL -> IO ()
doQuery stmt mysql__ = withForeignPtr mysql__ $ \mysql_ -> do
    withCString stmt $ \stmt_ -> do
      rv <- mysql_query mysql_ stmt_
      when (rv /= 0) (connectionError mysql_)

doCommit :: ForeignPtr MYSQL -> IO ()
doCommit = doQuery "COMMIT"

doRollback :: ForeignPtr MYSQL -> IO ()
doRollback = doQuery "ROLLBACK"

doStartTransaction :: ForeignPtr MYSQL -> IO ()
doStartTransaction = doQuery "START TRANSACTION"

-- Retrieve all the tables in the current database by issuing a "SHOW
-- TABLES" statement.
doGetTables :: ForeignPtr MYSQL -> IO [String]
doGetTables mysql__ = do
  stmt <- newStatement mysql__ "SHOW TABLES"
  _ <- Types.execute stmt []
  rows <- unfoldRows stmt
  Types.finish stmt
  return $ map (fromSql . head) rows
      where fromSql :: Types.SqlValue -> String
            fromSql (Types.SqlByteString s) = toString s
            fromSql _                       = error "SHOW TABLES returned a table whose name wasn't a string"

-- Describe a single table in the database by issuing a "DESCRIBE"
-- statement and parsing the results.  (XXX this is sloppy right now;
-- ideally you'd come up with exactly the same results as if you did a
-- describeResult on SELECT * FROM table.)
doDescribeTable :: ForeignPtr MYSQL -> String -> IO [(String, ColTypes.SqlColDesc)]
doDescribeTable mysql__ table = do
  stmt <- newStatement mysql__ ("DESCRIBE " ++ table)
  _ <- Types.execute stmt []
  rows <- unfoldRows stmt
  Types.finish stmt
  return $ map fromRow rows
      where fromRow :: [Types.SqlValue] -> (String, ColTypes.SqlColDesc)
            fromRow ((Types.SqlByteString colname)
                     :(Types.SqlByteString coltype)
                     :(Types.SqlByteString nullAllowed):_) =
                let sqlTypeId = typeIdOfString $ toString coltype
                    -- XXX parse the column width and decimals, too!
                    nullable = Just $ toString nullAllowed == "YES"
                in (toString colname, ColTypes.SqlColDesc sqlTypeId Nothing Nothing Nothing nullable)

            fromRow _ = error "DESCRIBE failed"

-- XXX this is likely to be incomplete.
typeIdOfString :: String -> ColTypes.SqlTypeId
typeIdOfString s
    | "int"       `isPrefixOf` s = ColTypes.SqlIntegerT
    | "bigint"    `isPrefixOf` s = ColTypes.SqlIntegerT
    | "smallint"  `isPrefixOf` s = ColTypes.SqlSmallIntT
    | "mediumint" `isPrefixOf` s = ColTypes.SqlIntegerT
    | "tinyint"   `isPrefixOf` s = ColTypes.SqlTinyIntT
    | "decimal"   `isPrefixOf` s = ColTypes.SqlDecimalT
    | "double"    `isPrefixOf` s = ColTypes.SqlDoubleT
    | "float"     `isPrefixOf` s = ColTypes.SqlFloatT
    | "char"      `isPrefixOf` s = ColTypes.SqlCharT
    | "varchar"   `isPrefixOf` s = ColTypes.SqlVarCharT
    | "text"      `isPrefixOf` s = ColTypes.SqlBinaryT
    | "timestamp" `isPrefixOf` s = ColTypes.SqlTimestampT
    | "datetime"  `isPrefixOf` s = ColTypes.SqlTimestampT
    | "date"      `isPrefixOf` s = ColTypes.SqlDateT
    | "time"      `isPrefixOf` s = ColTypes.SqlTimeT
    | otherwise                  = ColTypes.SqlUnknownT s

-- A helper function that turns an executed statement into the
-- resulting rows.
unfoldRows :: Types.Statement -> IO [[Types.SqlValue]]
unfoldRows stmt = do
  row <- Types.fetchRow stmt
  case row of
    Nothing     -> return []
    Just (vals) -> do rows <- unfoldRows stmt
                      return (vals : rows)

-- Returns the last statement-level error.
statementError :: Ptr MYSQL_STMT -> IO a
statementError stmt_ = do
  errno <- mysql_stmt_errno stmt_
  msg <- peekCString =<< mysql_stmt_error stmt_
  throwSqlError $ Types.SqlError "" (fromIntegral errno) msg

-- Returns the last connection-level error.
connectionError :: Ptr MYSQL -> IO a
connectionError mysql_ = do
  errno <- mysql_errno mysql_
  msg <- peekCString =<< mysql_error mysql_
  throwSqlError $ Types.SqlError "" (fromIntegral errno) msg

{- ---------------------------------------------------------------------- -}

mysql_get_client_info :: IO CString
mysql_get_client_info =
#if DEBUG
  trace "mysql_get_client_info"
#endif
    mysql_get_client_info_

mysql_get_server_info :: Ptr MYSQL -> IO CString
mysql_get_server_info =
#if DEBUG
  trace "mysql_get_server_info"
#endif
    mysql_get_server_info_

mysql_get_proto_info :: Ptr MYSQL -> IO CUInt
mysql_get_proto_info =
#if DEBUG
  trace "mysql_get_proto_info"
#endif
    mysql_get_proto_info_

mysql_init :: Ptr MYSQL -> IO (Ptr MYSQL)
mysql_init =
#if DEBUG
  trace "mysql_init"
#endif
    mysql_init_

mysql_set_character_set :: Ptr MYSQL -> CString -> IO CInt
mysql_set_character_set =
#if DEBUG
    trace "mysql_set_character_set_"
#endif
    mysql_set_character_set_

mysql_options :: Ptr MYSQL -> CInt -> Ptr () -> IO CInt
mysql_options =
#if DEBUG
  trace "mysql_options"
#endif
    mysql_options_

mysql_real_connect :: Ptr MYSQL -> CString -> CString -> CString -> CString -> CInt -> CString -> CULong -> IO (Ptr MYSQL)
mysql_real_connect =
#if DEBUG
  trace "mysql_real_connect"
#endif
    mysql_real_connect_

mysql_close :: FunPtr (Ptr MYSQL -> IO ())
mysql_close =
#if DEBUG
  trace "mysql_close"
#endif
    mysql_close_

mysql_stmt_init :: Ptr MYSQL -> IO (Ptr MYSQL_STMT)
mysql_stmt_init =
#if DEBUG
  trace "mysql_stmt_init"
#endif
    mysql_stmt_init_

mysql_stmt_prepare :: Ptr MYSQL_STMT -> CString -> CInt -> IO CInt
mysql_stmt_prepare =
#if DEBUG
  trace "mysql_stmt_prepare"
#endif
    mysql_stmt_prepare_

mysql_stmt_result_metadata :: Ptr MYSQL_STMT -> IO (Ptr MYSQL_RES)
mysql_stmt_result_metadata =
#if DEBUG
  trace "mysql_stmt_result_metadata"
#endif
    mysql_stmt_result_metadata_

mysql_stmt_bind_param :: Ptr MYSQL_STMT -> Ptr MYSQL_BIND -> IO CChar
mysql_stmt_bind_param =
#if DEBUG
  trace "mysql_stmt_bind_param"
#endif
    mysql_stmt_bind_param_

mysql_stmt_bind_result :: Ptr MYSQL_STMT -> Ptr MYSQL_BIND -> IO CChar
mysql_stmt_bind_result =
#if DEBUG
  trace "mysql_stmt_bind_result"
#endif
    mysql_stmt_bind_result_

mysql_stmt_param_count :: Ptr MYSQL_STMT -> IO CULong
mysql_stmt_param_count =
#if DEBUG
  trace "mysql_stmt_param_count"
#endif
    mysql_stmt_param_count_

mysql_free_result :: Ptr MYSQL_RES -> IO ()
mysql_free_result =
#if DEBUG
  trace "mysql_free_result"
#endif
    mysql_free_result_

mysql_stmt_execute :: Ptr MYSQL_STMT -> IO CInt
mysql_stmt_execute =
#if DEBUG
  trace "mysql_stmt_execute"
#endif
    mysql_stmt_execute_

mysql_stmt_affected_rows :: Ptr MYSQL_STMT -> IO CULLong
mysql_stmt_affected_rows =
#if DEBUG
  trace "mysql_stmt_affected_rows"
#endif
    mysql_stmt_affected_rows_

mysql_fetch_field :: Ptr MYSQL_RES -> IO (Ptr MYSQL_FIELD)
mysql_fetch_field =
#if DEBUG
  trace "mysql_fetch_field"
#endif
    mysql_fetch_field_

mysql_stmt_fetch :: Ptr MYSQL_STMT -> IO CInt
mysql_stmt_fetch =
#if DEBUG
  trace "mysql_stmt_fetch"
#endif
    mysql_stmt_fetch_

mysql_stmt_fetch_column :: Ptr MYSQL_STMT -> Ptr MYSQL_BIND -> CUInt -> CULong -> IO CInt
mysql_stmt_fetch_column =
#if DEBUG
  trace "mysql_stmt_fetch_column"
#endif
    mysql_stmt_fetch_column_

mysql_stmt_close :: Ptr MYSQL_STMT -> IO ()
mysql_stmt_close =
#if DEBUG
  trace "mysql_stmt_close"
#endif
    mysql_stmt_close_

mysql_stmt_errno :: Ptr MYSQL_STMT -> IO CInt
mysql_stmt_errno =
#if DEBUG
  trace "mysql_stmt_errno"
#endif
    mysql_stmt_errno_

mysql_stmt_error :: Ptr MYSQL_STMT -> IO CString
mysql_stmt_error =
#if DEBUG
  trace "mysql_stmt_error"
#endif
    mysql_stmt_error_

mysql_errno :: Ptr MYSQL -> IO CInt
mysql_errno =
#if DEBUG
  trace "mysql_errno"
#endif
    mysql_errno_

mysql_error :: Ptr MYSQL -> IO CString
mysql_error =
#if DEBUG
  trace "mysql_error"
#endif
    mysql_error_

mysql_autocommit :: Ptr MYSQL -> CChar -> IO CChar
mysql_autocommit =
#if DEBUG
  trace "mysql_autocommit"
#endif
    mysql_autocommit_

mysql_query :: Ptr MYSQL -> CString -> IO CInt
mysql_query =
#if DEBUG
  trace "mysql_query"
#endif
    mysql_query_

-- Here are all the FFI imports.

foreign import ccall unsafe "mysql_get_client_info" mysql_get_client_info_
    :: IO CString

foreign import ccall unsafe "mysql_get_server_info" mysql_get_server_info_
    :: Ptr MYSQL -> IO CString

foreign import ccall unsafe "mysql_get_proto_info" mysql_get_proto_info_
    :: Ptr MYSQL -> IO CUInt

foreign import ccall unsafe "mysql_init" mysql_init_
 :: Ptr MYSQL
 -> IO (Ptr MYSQL)

foreign import ccall unsafe "mysql_set_character_set" mysql_set_character_set_
 :: Ptr MYSQL -> CString -> IO CInt

foreign import ccall unsafe "mysql_options" mysql_options_
 :: Ptr MYSQL
 -> CInt
 -> Ptr ()
 -> IO CInt

foreign import ccall unsafe "mysql_real_connect" mysql_real_connect_
 :: Ptr MYSQL -- the context
 -> CString   -- hostname
 -> CString   -- username
 -> CString   -- password
 -> CString   -- database
 -> CInt      -- port
 -> CString   -- unix socket
 -> CULong    -- flags
 -> IO (Ptr MYSQL)

foreign import ccall unsafe "&mysql_close" mysql_close_
    :: FunPtr (Ptr MYSQL -> IO ())

foreign import ccall unsafe "mysql_stmt_init" mysql_stmt_init_
    :: Ptr MYSQL -> IO (Ptr MYSQL_STMT)

foreign import ccall unsafe "mysql_stmt_prepare" mysql_stmt_prepare_
    :: Ptr MYSQL_STMT -> CString -> CInt -> IO CInt

foreign import ccall unsafe "mysql_stmt_result_metadata" mysql_stmt_result_metadata_
    :: Ptr MYSQL_STMT -> IO (Ptr MYSQL_RES)

foreign import ccall unsafe "mysql_stmt_bind_param" mysql_stmt_bind_param_
    :: Ptr MYSQL_STMT -> Ptr MYSQL_BIND -> IO CChar

foreign import ccall unsafe "mysql_stmt_bind_result" mysql_stmt_bind_result_
    :: Ptr MYSQL_STMT -> Ptr MYSQL_BIND -> IO CChar

foreign import ccall unsafe "mysql_stmt_param_count" mysql_stmt_param_count_
    :: Ptr MYSQL_STMT -> IO CULong

foreign import ccall unsafe "mysql_free_result" mysql_free_result_
    :: Ptr MYSQL_RES -> IO ()

foreign import ccall unsafe "mysql_stmt_execute" mysql_stmt_execute_
    :: Ptr MYSQL_STMT -> IO CInt

foreign import ccall unsafe "mysql_stmt_affected_rows" mysql_stmt_affected_rows_
    :: Ptr MYSQL_STMT -> IO CULLong

foreign import ccall unsafe "mysql_fetch_field" mysql_fetch_field_
    :: Ptr MYSQL_RES -> IO (Ptr MYSQL_FIELD)

foreign import ccall unsafe "mysql_stmt_fetch" mysql_stmt_fetch_
    :: Ptr MYSQL_STMT -> IO CInt

foreign import ccall unsafe "mysql_stmt_fetch_column" mysql_stmt_fetch_column_
    :: Ptr MYSQL_STMT -> Ptr MYSQL_BIND -> CUInt -> CULong -> IO CInt

foreign import ccall unsafe "mysql_stmt_close" mysql_stmt_close_
    :: Ptr MYSQL_STMT -> IO ()

foreign import ccall unsafe "mysql_stmt_errno" mysql_stmt_errno_
    :: Ptr MYSQL_STMT -> IO CInt

foreign import ccall unsafe "mysql_stmt_error" mysql_stmt_error_
    :: Ptr MYSQL_STMT -> IO CString

foreign import ccall unsafe "mysql_errno" mysql_errno_
    :: Ptr MYSQL -> IO CInt

foreign import ccall unsafe "mysql_error" mysql_error_
    :: Ptr MYSQL -> IO CString

foreign import ccall unsafe "mysql_autocommit" mysql_autocommit_
    :: Ptr MYSQL -> CChar -> IO CChar

foreign import ccall unsafe "mysql_query" mysql_query_
    :: Ptr MYSQL -> CString -> IO CInt

foreign import ccall unsafe memset
    :: Ptr () -> CInt -> CSize -> IO ()

