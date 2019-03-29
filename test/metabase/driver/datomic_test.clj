(ns metabase.driver.datomic-test
  (:require [clojure.test :refer :all]
            [metabase.driver :as driver]
            [metabase.test.data.interface :as tx]
            [metabase.test.data.datomic]
            [metabase.test.util :as tu]
            [metabase.test.data :as data]
            [toucan.db :as db]
            [metabase.models.table :refer [Table]]
            [metabase.models.field :refer [Field]]
            [metabase.query-processor :as qp]))

(tx/def-database-definition countries
  [["country"
    [{:field-name "code" :base-type :type/Text}
     {:field-name "name" :base-type :type/Text}]
    [["BE" "Belgium"]
     ["DE" "Germany"]
     ["FI" "Finnland"]]]])

(tx/def-database-definition aggr-data
  [["aggr"
    [{:field-name "f1" :base-type :type/Text}
     {:field-name "f2" :base-type :type/Text}]
    [["xxx" "a"] ["xxx" "b"] ["yyy" "c"]]]])

(defn rows+cols [result]
  (select-keys (:data result) [:columns :rows]))

(deftest basic-query-test
  (driver/with-driver :datomic
    (data/with-temp-db [_ countries]
      (is (= {:columns ["db/id" "country/code" "country/name"],
              :rows [[17592186045418 "BE" "Belgium"]
                     [17592186045419 "DE" "Germany"]
                     [17592186045420 "FI" "Finnland"]]}
             (rows+cols (data/run-mbql-query country))))))


  )

(driver/with-driver :datomic
  (data/with-temp-db [_ aggr-data]
    (data/run-mbql-query aggr
      {:aggregation [[:count]]
       :breakout [[:field-id (data/id "aggr" "aggr/f1")]]
       })))


(data/run-mbql-query country)
(driver/with-driver :datomic
  (data/with-temp-db [_ aggr-data]
    (qp/query->native
      (data/mbql-query aggr
        {;;:breakout [[:field-id (data/id "aggr" "aggr/f1")]]
         :fields [[:field-id (data/id "aggr" "aggr/f1")]]
         :aggregation [[:count]]
         :order-by [[:desc [:aggregation 0]] [:asc [:field-id 126]]]}))))
