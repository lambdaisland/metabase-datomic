(ns metabase.driver.datomic.query-processor-test
  (:require [metabase.driver.datomic.query-processor :as qp]
            [clojure.test :refer :all]
            [metabase.test.data :as data]
            [toucan.db :as db]
            [metabase.models.database :as database]))


#_
(data/with-db (db/select-one database/Database)
  (qp/mbql->native {:query
                    {:source-table x}})
  )
