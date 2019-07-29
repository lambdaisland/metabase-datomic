(ns schema
  (:require [metabase.driver.datomic :as datomic-driver]
            [metabase.driver.datomic.query-processor :as datomic.qp]
            [metabase.models.database :refer [Database]]
            [toucan.db :as db]))

(user/refer-repl)

(datomic.qp/table-columns (db eeleven-url) "journal-entry-line")

(filter (comp #{"account"} :name)
        (:fields (datomic-driver/describe-table (db/select-one Database :name "Eleven SG") {:name "journal-entry-line"})))
