(ns metabase.driver.datomic-test
  (:require [clojure.test :refer :all]
            [metabase.driver :as driver]
            [metabase.test.data.interface :as tx]
            [metabase.test.util :as tu]
            [metabase.test.data :as data]
            [toucan.db :as db]
            [metabase.models.table :refer [Table]]))

(tx/def-database-definition sample-data
  [["country"
    [{:field-name "code" :base-type :type/Text}
     {:field-name "name" :base-type :type/Text}]
    [["BE" "Belgium"]
     ["DE" "Germany"]
     ["FI" "Finnland"]]]])


(driver/with-driver :datomic
  (data/dataset sample-data
    (doall (data/run-mbql-query country)))
  )

(db/select Table)

(db/delete! metabase.models.database/Database
  :id
  (->> metabase.models.database/Database
       db/select
       (filter (comp #{"sample-data"} :name))
       first
       :id))

#_
(expect-with-driver :datomic
  "UTC"
  (tu/db-timezone-id))
