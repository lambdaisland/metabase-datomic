(ns metabase.driver.datomic.test
  (:require [metabase.driver :as driver]
            [metabase.query-processor :as qp]))

(require 'metabase.driver.datomic.test-data)
(require 'matcher-combinators.test)

(defmacro with-datomic [& body]
  `(driver/with-driver :datomic
     ~@body))

(def query->native
  (comp read-string :query qp/query->native))
