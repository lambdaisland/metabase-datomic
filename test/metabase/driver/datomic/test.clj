(ns metabase.driver.datomic.test
  (:require [metabase.driver :as driver]))

(defmacro with-datomic [& body]
  `(driver/with-driver :datomic
     ~@body))
