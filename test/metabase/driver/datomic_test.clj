(ns metabase.driver.datomic-test
  (:require [clojure.test :refer :all]
            [metabase.driver :as driver]
            [metabase.test.data.interface :as tx]
            [metabase.test.data.datomic]
            [metabase.test.util :as tu]
            [metabase.test.data :as data]
            [toucan.db :as db]
            [metabase.models.table :refer [Table]]
            [metabase.models.field :refer [Field]]))

(tx/def-database-definition sample-data
  [["country"
    [{:field-name "code" :base-type :type/Text}
     {:field-name "name" :base-type :type/Text}]
    [["BE" "Belgium"]
     ["DE" "Germany"]
     ["FI" "Finnland"]]]])

(defn rows+cols [result]
  (select-keys (:data result) [:columns :rows]))

(deftest basic-query-test
  (driver/with-driver :datomic
    (data/with-temp-db [_ sample-data]

      (is (= {:columns ["db/id" "country/code" "country/name"],
              :rows [[17592186045418 "BE" "Belgium"]
                     [17592186045419 "DE" "Germany"]
                     [17592186045420 "FI" "Finnland"]]}
             (rows+cols (data/run-mbql-query country)))))))
