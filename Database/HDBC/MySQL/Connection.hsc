-- -*- mode: haskell; -*-
{-# OPTIONS -fglasgow-exts #-}

module Database.HDBC.MySQL.Connection
    (connectMySQL, MySQLConnectInfo(..), defaultMySQLConnectInfo)
where

import Control.Exception
import Control.Monad
import Foreign
import Foreign.C
import qualified Data.ByteString as B
import Data.IORef
import Data.List (isPrefixOf)
import Data.Time
import Data.Time.Clock.POSIX

import qualified Database.HDBC.Types as Types
import Database.HDBC.ColTypes as ColTypes

#include <mysql.h>

{- | Connection information to use with connectMySQL.

     You must either supply a host and port, or the name of a Unix
     socket.

-}
data MySQLConnectInfo = MySQLConnectInfo
    { -- | e.g., @\"db1.example.com\"@
      mysqlHost       :: String
      -- | e.g., @\"scott\"@
    , mysqlUser       :: String
      -- | e.g., @\"tiger\"@
    , mysqlPassword   :: String
      -- | the \"default\" database name; e.g., @\"emp\"@
    , mysqlDatabase   :: String
      -- | e.g., @3306@
    , mysqlPort       :: Int
      -- | e.g., @\"\/var\/lib\/mysql.sock\"@
    , mysqlUnixSocket :: String
    }

{- | Typical connection information, meant to be overridden partially,
     for example:

     > connectMySQL defaultMySQLConnectInfo { mysqlHost = "db1" }

     In particular, the default values are @\"127.0.0.1\"@ as the
     host, @3306@ as the port, @\"root\"@ as the user, no password,
     and @\"test\"@ as the default database.

-}
defaultMySQLConnectInfo :: MySQLConnectInfo
defaultMySQLConnectInfo = MySQLConnectInfo "127.0.0.1" "root" "" "test" 3306 ""

data Connection = Connection
    { disconnect :: IO ()
    , commit :: IO ()
    , rollback :: IO ()
    , run :: String -> [Types.SqlValue] -> IO Integer
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
  withCString (mysqlHost info) $ \host_ ->
      withCString (mysqlUser info) $ \user_ ->
          withCString (mysqlPassword info) $ \passwd_ ->
              withCString (mysqlDatabase info) $ \db_ ->
                  withCString (mysqlUnixSocket info) $ \unixSocket_ ->
                      do rv <- mysql_real_connect mysql_ host_ user_ passwd_ db_
                                                  (fromIntegral $ mysqlPort info)
                                                  unixSocket_
                         when (rv == nullPtr) (connectionError mysql_)
                         wrap mysql_
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
        mysql_autocommit mysql_ 0

        mysql__ <- newForeignPtr mysql_close mysql_
        doStartTransaction mysql__

        return $ Connection
                   { disconnect           = finalizeForeignPtr mysql__
                   , commit               = doCommit mysql__ >> doStartTransaction mysql__
                   , rollback             = doRollback mysql__ >> doStartTransaction mysql__
                   , run                  = doRun mysql__
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
    }

instance Storable MYSQL_BIND where
    sizeOf _      = #const sizeof(MYSQL_BIND)
    alignment _   = alignment (undefined :: CInt)

    peek _ = error "MYSQL_BIND: peek"

    poke p (MYSQL_BIND len_ isNull_ buf_ err_ buftyp buflen) = do
        memset (castPtr p) 0 #{const sizeof(MYSQL_BIND)}
        (#poke MYSQL_BIND, length)        p len_
        (#poke MYSQL_BIND, is_null)       p isNull_
        (#poke MYSQL_BIND, buffer)        p buf_
        (#poke MYSQL_BIND, error)         p err_
        (#poke MYSQL_BIND, buffer_type)   p buftyp
        (#poke MYSQL_BIND, buffer_length) p buflen

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
  stmt__ <- newForeignPtr mysql_stmt_close stmt_

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

  -- This is mildly insane.  Create an IORef where we can store the
  -- finalizer, so we can later free the finalizer.
  gptr <- newIORef nullFunPtr
  g <- makeFinalizer $ freeBinds results gptr
  writeIORef gptr g
  addForeignPtrFinalizer g stmt__

  -- We pass the connection ForeignPtr down to execute and fetchRow as
  -- a silly way to keep a reference to it alive so long as the
  -- statement is around.
  return $ Types.Statement
             { Types.execute        = execute mysql__ stmt__
             , Types.executeMany    = mapM_ $ execute mysql__ stmt__
             , Types.finish         = finalizeForeignPtr stmt__
             , Types.fetchRow       = fetchRow mysql__ stmt__ results
             , Types.originalQuery  = query
             , Types.getColumnNames = return $ map fieldName fields
             , Types.describeResult = return $ map sqlColDescOf fields
             }

type Finalizer a = Ptr a -> IO ()
foreign import ccall "wrapper" makeFinalizer
    :: Finalizer a -> IO (FunPtr (Finalizer a))

-- Release the storage allocated for each bind.
freeBinds :: [MYSQL_BIND] -> IORef (FunPtr (Finalizer a)) -> Ptr MYSQL_STMT -> IO ()
freeBinds binds selfPtrRef _ = do
  readIORef selfPtrRef >>= freeHaskellFunPtr
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
          bindParams stmt_ params
          rv <- mysql_stmt_execute stmt_
          when (rv /= 0) (statementError stmt_)
          nrows <- mysql_stmt_affected_rows stmt_

          -- mysql_stmt_affected_rows returns -1 when called on a
          -- SELECT statement; the HDBC API expects zero to be
          -- returned in this case.
          return $ fromIntegral (if nrows == (-1 :: CULLong) then 0 else nrows)

-- Binds placeholder parameters to values.
bindParams :: Ptr MYSQL_STMT -> [Types.SqlValue] -> IO ()
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

-- Given a SqlValue, return a MYSQL_BIND structure that we can use to
-- pass its value.
bindOfSqlValue :: Types.SqlValue -> IO MYSQL_BIND

bindOfSqlValue Types.SqlNull =
    with (1 :: CChar) $ \isNull_ ->
        return $ MYSQL_BIND
                   { bindLength       = nullPtr
                   , bindIsNull       = isNull_
                   , bindBuffer       = nullPtr
                   , bindError        = nullPtr
                   , bindBufferType   = #{const MYSQL_TYPE_NULL}
                   , bindBufferLength = 0
                   }

bindOfSqlValue (Types.SqlString s) =
    -- XXX this might not handle embedded null characters correctly.
    bindOfSqlValue' (length s) (withCString s) #{const MYSQL_TYPE_VAR_STRING}

bindOfSqlValue (Types.SqlByteString s) =
    bindOfSqlValue' (B.length s) (B.useAsCString s) #{const MYSQL_TYPE_VAR_STRING}

bindOfSqlValue (Types.SqlInteger n) =
    bindOfSqlValue' (8::Int) (with (fromIntegral n :: CLLong)) #{const MYSQL_TYPE_LONGLONG}

bindOfSqlValue (Types.SqlBool b) =
    bindOfSqlValue' (1::Int) (with (if b then 1 else 0 :: CChar)) #{const MYSQL_TYPE_TINY}

bindOfSqlValue (Types.SqlChar c) =
    bindOfSqlValue' (1::Int) (with c) #{const MYSQL_TYPE_TINY}

bindOfSqlValue (Types.SqlDouble d) =
    bindOfSqlValue' (8::Int) (with (realToFrac d :: CDouble)) #{const MYSQL_TYPE_DOUBLE}

bindOfSqlValue (Types.SqlInt32 n) =
    bindOfSqlValue' (4::Int) (with n) #{const MYSQL_TYPE_LONG}

bindOfSqlValue (Types.SqlInt64 n) =
    bindOfSqlValue' (8::Int) (with n) #{const MYSQL_TYPE_LONGLONG}

bindOfSqlValue (Types.SqlRational n) =
    bindOfSqlValue' (8::Int) (with (realToFrac n :: CDouble)) #{const MYSQL_TYPE_DOUBLE}

bindOfSqlValue (Types.SqlWord32 n) =
    bindOfSqlValue' (4::Int) (with n) #{const MYSQL_TYPE_LONG}

bindOfSqlValue (Types.SqlWord64 n) =
    bindOfSqlValue' (8::Int) (with n) #{const MYSQL_TYPE_LONGLONG}

bindOfSqlValue (Types.SqlEpochTime epoch) =
    let t = utcToMysqlTime $ posixSecondsToUTCTime (fromIntegral epoch) in
    bindOfSqlValue' (#{const sizeof(MYSQL_TIME)}::Int) (with t) #{const MYSQL_TYPE_DATETIME}
        where utcToMysqlTime :: UTCTime -> MYSQL_TIME
              utcToMysqlTime (UTCTime day difftime) =
                  let (y, m, d) = toGregorian day
                      t  = floor $ (realToFrac difftime :: Double)
                      h  = t `div` 3600
                      mn = t `div` 60 `mod` 60
                      s  = t `mod` 60
                  in MYSQL_TIME (fromIntegral y) (fromIntegral m) (fromIntegral d) h mn s

bindOfSqlValue (Types.SqlTimeDiff n) =
    let h  = fromIntegral $ n `div` 3600
        mn = fromIntegral $ n `div` 60 `mod` 60
        s  = fromIntegral $ n `mod` 60
        t  = MYSQL_TIME 0 0 0 h mn s in
    bindOfSqlValue' (#{const sizeof(MYSQL_TIME)}::Int) (with t) #{const MYSQL_TYPE_TIME}

-- A nasty helper function that cuts down on the boilerplate a bit.
bindOfSqlValue' :: (Integral a, Storable b) =>
                   a ->
                       ((Ptr b -> IO MYSQL_BIND) -> IO MYSQL_BIND) ->
                           CInt ->
                               IO MYSQL_BIND

bindOfSqlValue' len buf btype =
    let buflen = fromIntegral len in
    with (0 :: CChar) $ \isNull_ ->
        with buflen $ \len_ ->
            buf $ \buf_ ->
                return $ MYSQL_BIND
                           { bindLength       = len_
                           , bindIsNull       = isNull_
                           , bindBuffer       = castPtr buf_
                           , bindError        = nullPtr
                           , bindBufferType   = btype
                           , bindBufferLength = buflen
                           }

-- Returns an appropriate binding structure for a field.
resultOfField :: MYSQL_FIELD -> IO MYSQL_BIND
resultOfField field =
    let ftype = fieldType field
        btype = boundType ftype (fieldDecimals field)
        size  = boundSize btype (fieldLength field) in
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
boundType #{const MYSQL_TYPE_DATE}       _ = #{const MYSQL_TYPE_DATETIME}
boundType #{const MYSQL_TYPE_TIMESTAMP}  _ = #{const MYSQL_TYPE_DATETIME}
boundType #{const MYSQL_TYPE_NEWDATE}    _ = #{const MYSQL_TYPE_DATETIME}
boundType #{const MYSQL_TYPE_BLOB}       _ = #{const MYSQL_TYPE_VAR_STRING}
boundType t                              _ = t

-- Returns the amount of storage required for a particular result
-- type.
boundSize :: CInt -> CULong -> CULong
boundSize #{const MYSQL_TYPE_LONG}     _ = 4
boundSize #{const MYSQL_TYPE_DOUBLE}   _ = 8
boundSize #{const MYSQL_TYPE_DATETIME} _ = #{const sizeof(MYSQL_TIME)}
boundSize _                            n = n

-- Fetches a row from an executed statement and converts the cell
-- values into a list of SqlValue types.
fetchRow :: ForeignPtr MYSQL -> ForeignPtr MYSQL_STMT -> [MYSQL_BIND] -> IO (Maybe [Types.SqlValue])
fetchRow mysql__ stmt__ results =
  withForeignPtr mysql__ $ \_ ->
      withForeignPtr stmt__ $ \stmt_ -> do
          rv <- mysql_stmt_fetch stmt_
          case rv of
            0                             -> row
            #{const MYSQL_DATA_TRUNCATED} -> row
            #{const MYSQL_NO_DATA}        -> finalizeForeignPtr stmt__ >> return Nothing
            _                             -> statementError stmt_
    where row = mapM cellValue results >>= \cells -> return $ Just cells

-- Produces a single SqlValue cell value given the binding, handling
-- nulls appropriately.
cellValue :: MYSQL_BIND -> IO Types.SqlValue
cellValue bind = do
  isNull <- peek $ bindIsNull bind
  if isNull == 0 then cellValue' else return Types.SqlNull
      where cellValue' = do
                   len <- peek $ bindLength bind
                   let buftype = bindBufferType bind
                       buf     = bindBuffer bind
                   nonNullCellValue buftype buf len

-- Produces a single SqlValue from the binding's type and buffer
-- pointer.  It assumes that the value is not null.
nonNullCellValue :: CInt -> Ptr () -> CULong -> IO Types.SqlValue

nonNullCellValue #{const MYSQL_TYPE_LONG} p _ = do
  n :: CInt <- peek $ castPtr p
  return $ Types.SqlInteger (fromIntegral n)

nonNullCellValue #{const MYSQL_TYPE_LONGLONG} p _ = do
  n :: CLLong <- peek $ castPtr p
  return $ Types.SqlInteger (fromIntegral n)

nonNullCellValue #{const MYSQL_TYPE_DOUBLE} p _ = do
  n :: CDouble <- peek $ castPtr p
  return $ Types.SqlDouble (realToFrac n)

nonNullCellValue #{const MYSQL_TYPE_VAR_STRING} p len =
    peekCStringLen ((castPtr p), fromIntegral len) >>= return . Types.SqlString

nonNullCellValue #{const MYSQL_TYPE_DATETIME} p _ = do
  t :: MYSQL_TIME <- peek $ castPtr p
  let epoch = (floor . toRational . utcTimeToPOSIXSeconds . mysqlTimeToUTC) t
  return $ Types.SqlEpochTime epoch
      where mysqlTimeToUTC :: MYSQL_TIME -> UTCTime
            mysqlTimeToUTC (MYSQL_TIME y m d h mn s) =
                -- XXX so, this is fine if the date we're getting back
                -- is UTC.  If not, well, it's wrong.
                let day = fromGregorian (fromIntegral y) (fromIntegral m) (fromIntegral d)
                    time = s + mn * 60 + h * 3600
                in UTCTime day (secondsToDiffTime $ fromIntegral time)

nonNullCellValue #{const MYSQL_TYPE_TIME} p _ = do
  (MYSQL_TIME _ _ _ h mn s) <- peek $ castPtr p
  let secs = 3600 * h + 60 * mn + s
  return $ Types.SqlTimeDiff (fromIntegral secs)

nonNullCellValue t _ _ = return $ Types.SqlString ("unknown type " ++ show t)

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
  Types.execute stmt []
  rows <- unfoldRows stmt
  Types.finish stmt
  return $ map (fromSql . head) rows
      where fromSql :: Types.SqlValue -> String
            fromSql (Types.SqlString s) = s
            fromSql _                   = error "SHOW TABLES returned a table whose name wasn't a string"

-- Describe a single table in the database by issuing a "DESCRIBE"
-- statement and parsing the results.  (XXX this is sloppy right now;
-- ideally you'd come up with exactly the same results as if you did a
-- describeResult on SELECT * FROM table.)
doDescribeTable :: ForeignPtr MYSQL -> String -> IO [(String, ColTypes.SqlColDesc)]
doDescribeTable mysql__ table = do
  stmt <- newStatement mysql__ ("DESCRIBE " ++ table)
  Types.execute stmt []
  rows <- unfoldRows stmt
  Types.finish stmt
  return $ map fromRow rows
      where fromRow :: [Types.SqlValue] -> (String, ColTypes.SqlColDesc)
            fromRow ((Types.SqlString colname)
                     :(Types.SqlString coltype)
                     :(Types.SqlString nullAllowed):_) =
                let sqlTypeId = typeIdOfString coltype
                    -- XXX parse the column width and decimals, too!
                    nullable = Just $ nullAllowed == "YES"
                in (colname, ColTypes.SqlColDesc sqlTypeId Nothing Nothing Nothing nullable)

            fromRow _ = throwDyn $ Types.SqlError "" 0 "DESCRIBE failed"

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
  throwDyn $ Types.SqlError "" (fromIntegral errno) msg

-- Returns the last connection-level error.
connectionError :: Ptr MYSQL -> IO a
connectionError mysql_ = do
  errno <- mysql_errno mysql_
  msg <- peekCString =<< mysql_error mysql_
  throwDyn $ Types.SqlError "" (fromIntegral errno) msg

{- ---------------------------------------------------------------------- -}

-- Here are all the FFI imports.

foreign import ccall unsafe mysql_get_client_info
    :: IO CString

foreign import ccall unsafe mysql_get_server_info
    :: Ptr MYSQL -> IO CString

foreign import ccall unsafe mysql_get_proto_info
    :: Ptr MYSQL -> IO CUInt

foreign import ccall unsafe mysql_init
 :: Ptr MYSQL
 -> IO (Ptr MYSQL)

foreign import ccall unsafe mysql_real_connect
 :: Ptr MYSQL -- the context
 -> CString   -- hostname
 -> CString   -- username
 -> CString   -- password
 -> CString   -- database
 -> CInt      -- port
 -> CString   -- unix socket
 -> IO (Ptr MYSQL)

foreign import ccall unsafe "&mysql_close" mysql_close
    :: FunPtr (Ptr MYSQL -> IO ())

foreign import ccall unsafe mysql_stmt_init
    :: Ptr MYSQL -> IO (Ptr MYSQL_STMT)

foreign import ccall unsafe mysql_stmt_prepare
    :: Ptr MYSQL_STMT -> CString -> CInt -> IO CInt

foreign import ccall unsafe mysql_stmt_result_metadata
    :: Ptr MYSQL_STMT -> IO (Ptr MYSQL_RES)

foreign import ccall unsafe mysql_stmt_bind_param
    :: Ptr MYSQL_STMT -> Ptr MYSQL_BIND -> IO CChar

foreign import ccall unsafe mysql_stmt_bind_result
    :: Ptr MYSQL_STMT -> Ptr MYSQL_BIND -> IO CChar

foreign import ccall unsafe mysql_stmt_param_count
    :: Ptr MYSQL_STMT -> IO CULong

foreign import ccall unsafe mysql_free_result
    :: Ptr MYSQL_RES -> IO ()

foreign import ccall unsafe mysql_stmt_execute
    :: Ptr MYSQL_STMT -> IO CInt

foreign import ccall unsafe mysql_stmt_affected_rows
    :: Ptr MYSQL_STMT -> IO CULLong

foreign import ccall unsafe mysql_fetch_field
    :: Ptr MYSQL_RES -> IO (Ptr MYSQL_FIELD)

foreign import ccall unsafe mysql_stmt_fetch
    :: Ptr MYSQL_STMT -> IO CInt

foreign import ccall unsafe "&mysql_stmt_close" mysql_stmt_close
    :: FunPtr (Ptr MYSQL_STMT -> IO ())

foreign import ccall unsafe mysql_stmt_errno
    :: Ptr MYSQL_STMT -> IO CInt

foreign import ccall unsafe mysql_stmt_error
    :: Ptr MYSQL_STMT -> IO CString

foreign import ccall unsafe mysql_errno
    :: Ptr MYSQL -> IO CInt

foreign import ccall unsafe mysql_error
    :: Ptr MYSQL -> IO CString

foreign import ccall unsafe mysql_autocommit
    :: Ptr MYSQL -> CChar -> IO CChar

foreign import ccall unsafe mysql_query
    :: Ptr MYSQL -> CString -> IO CInt

foreign import ccall unsafe memset
    :: Ptr () -> CInt -> CSize -> IO ()

