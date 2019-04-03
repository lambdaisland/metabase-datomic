(ns metabase.driver.datomic-test
  (:require [clojure.test :as test :refer [is are testing]]
            [metabase.driver :as driver]
            [metabase.test.data.interface :as tx]
            [metabase.test.data.datomic]
            [metabase.test.util :as tu]
            [metabase.test.data :as data]
            [toucan.db :as db]
            [metabase.models.table :refer [Table]]
            [metabase.models.field :refer [Field]]
            [metabase.query-processor :as qp]))

(require 'matcher-combinators.test)

(defmacro deftest [name & body]
  `(test/deftest ~name
     (driver/with-driver :datomic
       ~@body)))

(tx/def-database-definition countries
  [["country"
    [{:field-name "code" :base-type :type/Text}
     {:field-name "name" :base-type :type/Text}]
    [["BE" "Belgium"]
     ["DE" "Germany"]
     ["FI" "Finnland"]]]])
#_(remove-database "countries")

(tx/def-database-definition aggr-data
  [["foo"
    [{:field-name "f1" :base-type :type/Text}
     {:field-name "f2" :base-type :type/Text}]
    [["xxx" "a"] ["xxx" "b"] ["yyy" "c"]]]])

(defn rows+cols [result]
  (select-keys (:data result) [:columns :rows]))

(deftest basic-query-test
  (is (match? {:columns ["db/id" "country/code" "country/name"],
               :rows    [[pos-int? "BE" "Belgium"]
                         [pos-int? "DE" "Germany"]
                         [pos-int? "FI" "Finnland"]]}
              (rows+cols
               (data/dataset countries
                 (data/run-mbql-query country))))))

(deftest aggregrate-count-test
  (is (match? {:columns ["foo/f1" "count"],
               :rows    [["xxx" 2] ["yyy" 1]]}
              (rows+cols
               (data/dataset aggr-data
                 (data/run-mbql-query foo
                   {:aggregation [[:count]]
                    :breakout    [$f1]}))))))

#_
(driver/with-driver :datomic
  (data/with-temp-db [_ aggr-data]
    (qp/query->native
      (data/mbql-query aggr
        {;;:breakout [[:field-id (data/id "aggr" "aggr/f1")]]
         :fields [[:field-id (data/id "aggr" "aggr/f1")]]
         :aggregation [[:count]]
         :order-by [[:desc [:aggregation 0]] [:asc [:field-id 126]]]}))))
