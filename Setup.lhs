#!/usr/bin/env runhaskell

\begin{code}
{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances #-}
import Distribution.Simple
import Distribution.PackageDescription
import Distribution.Version

import Distribution.Simple.LocalBuildInfo
import Distribution.Simple.Program
import Distribution.Verbosity

import Data.Char (isSpace)
import Data.List (dropWhile,span)

import Control.Monad

main = defaultMainWithHooks simpleUserHooks {
  hookedPrograms = [mysqlConfigProgram],

  confHook = \pkg flags -> do
    lbi <- confHook simpleUserHooks pkg flags
    bi  <- mysqlBuildInfo lbi
    return lbi {
      localPkgDescr = updatePackageDescription
                        (Just bi, []) (localPkgDescr lbi)
    }
}

 
-- 'ConstOrId' is a @Cabal-1.16@ vs @Cabal-1.18@ compatibility hack,
-- 'programFindLocation' has a new (unused in this case)
-- parameter. 'ConstOrId' adds this parameter when types say it is
-- mandatory.

class ConstOrId a b where
    constOrId :: a -> b

instance ConstOrId a a where
    constOrId = id

instance ConstOrId a (b -> a) where
    constOrId = const


mysqlConfigProgram = (simpleProgram "mysql_config") {
    programFindLocation = \verbosity -> constOrId $ do
      mysql_config  <- findProgramOnPath "mysql_config"  verbosity
      mysql_config5 <- findProgramOnPath "mysql_config5" verbosity
      return (mysql_config `mplus` mysql_config5)
  }

mysqlBuildInfo :: LocalBuildInfo -> IO BuildInfo
mysqlBuildInfo lbi = do
  let mysqlConfig = rawSystemProgramStdoutConf verbosity mysqlConfigProgram (withPrograms lbi)
      ws = " \n\r\t"

  includeDirs <- return . map (drop 2) . split ws =<< mysqlConfig ["--include"]
  ldOptions   <- return . split ws =<< mysqlConfig ["--libs"]

  return emptyBuildInfo {
    ldOptions   = ldOptions,
    includeDirs = includeDirs
  }
  where
    verbosity = normal -- honestly, this is a hack

split :: Eq a => [a] -> [a] -> [[a]]
split xs cs = split' $ dropWhile (`elem` xs) cs
    where split' []  = []
          split' cs0 =
              let (run, cs1) = span (`notElem` xs) cs0
                  cs2        = dropWhile (`elem` xs) cs1
              in run:(split' cs2)
\end{code}
