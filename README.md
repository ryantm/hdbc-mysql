# HDBC-mysql

This is a "native" HDBC driver for MySQL that makes use of
libmysqlclient to communicate with a MySQL server.  By way of
synopsis:

```haskell
import Control.Monad
import Database.HDBC
import Database.HDBC.MySQL
main = do conn <- connectMySQL defaultMySQLConnectInfo {
                       mysqlHost     = "db1.example.com",
                       mysqlUser     = "scott",
                       mysqlPassword = "tiger"
                    }

          rows <- quickQuery' conn "SELECT 1 + 1" []
          forM_ rows $ \row -> putStrLn $ show row
```

At the moment, please consider this to be "alpha" software.  As far as
I can tell, it works.  There are some limitations that you should be
aware of.

## Caveats


  * This works with MySQL server and client libraries 5.0.75.  I
    haven't tried with 4.x nor 5.1.  I suspect that using a server
    version less than 4.1 won't work, due to lack of support for
    prepared statements.

  * MySQL DATETIME and TIMESTAMP columns have no time zone information,
    because they just don't.  So, I'm just converting them blindly to
    SqlEpochTime values, and assuming the times are UTC.  This is all
    fine if you're actually running your server in UTC, but it will
    probably be confusing if you're not.  It might be possible to
    interpret the times using the current connection's default
    time zone setting, instead.  Is that better?  Or worse?

  * Regardless, the types I convert to are now deprecated, and I need
    to implement the new types (SqlLocalDate, etc.)

  * Out of the box, MySQL probably uses MyISAM tables to store its
    data, and MyISAM tables don't support transactions.  Yet, I'm
    going to blindly respond "yes" if you ask whether the driver
    itself supports transactions, and assume that you know enough to
    use InnoDB tables in the database if you want to make use of
    HDBC's transactional support.  I *suppose* I might be able to
    discover what the default table type is, and say "no" if it's not
    a table type that supports transactions, but... meh.

  * I'm not sure that I can tell the difference between a MySQL TEXT
    and a MySQL BLOB column.  If you ask about the metadata of either,
    I'll tell you it's a SqlBinaryT.

  * The statement and table metadata could stand to be improved a bit.
    In particular, it would be nice if "describeTable foo" and
    "describeResults" on "SELECT * FROM foo" returned the same thing.
    (They're sorta close, I guess...)

  * Thread-safety could be an issue.  In the driver code, there's
    definitely a race condition between "prepare" and "disconnect",
    for example.  I haven't even *looked* at thread-safety issues for
    the MySQL driver.  I'm not sure if I should worry about it, or if
    we assume that's going to be dealt with at a higher level.

  * We might crash if someone opens a connection, prepares a
    statement, explicitly disconnects the connection, and then tries
    to play with the statement.

  * It probably makes sense to marshall to the SqlByteString type when
    retrieving BLOB data.

  * The only supported client charset is `utf8mb4`.

## Testing

There's a little test program that runs a query and spews out the
results.  To compile it,

```
ghc -idist/build -L/opt/local/lib/mysql5/mysql -lmysqlclient --make Test
```

I'm still trying to get the Makefile right so that it can build the
test sources: it's not there yet.  Here's how I've been doing it, for
now:

```
cd testsrc
ghc --make -package HUnit -package HDBC -Wall -i../dist/build -i.. -L/opt/local/lib/mysql5/mysql -lmysqlclient -o runtests runtests.hs
```

One issue is that I want the location of the MySQL library to come
from the configuration data, rather than be hard-coded.

## Developing hdbc-mysql with nix

```
# Nix shell
nix-shell -p ghc gcc mysql pkgconfig zlib openssl haskellPackages.haddock

# Use Cabal to build
cabal clean && cabal v2-build

# Build test script
ghc -idist/build -L/nix/store/l2yf2p42lqpbkjn428gx9w76dxbhaaga-mariadb-server-10.3.20/lib/ -lmysqlclient --make Test

# Run test script
./Test
```
