(ns metabase.driver.datomic-test
  (:require [clojure.test :as test :refer [deftest is are testing]]
            [metabase.driver.datomic.test :refer [with-datomic]]
            [metabase.driver :as driver]
            [metabase.driver.datomic.test-data :as test-data]
            [metabase.test.data.interface :as tx]
            [metabase.test.data.datomic]
            [metabase.test.util :as tu]
            [metabase.test.data :as data]
            [toucan.db :as db]
            [metabase.models.table :refer [Table]]
            [metabase.models.field :refer [Field]]
            [metabase.models.database :refer [Database]]
            [metabase.query-processor :as qp]))

(require 'matcher-combinators.test)

(deftest basic-query-test
  (with-datomic
    (is (match? {:columns ["db/id" "name" "code"]
                 :rows    [[pos-int? "Belgium" "BE"]
                           [pos-int? "Germany" "DE"]
                           [pos-int? "Finnland" "FI"]]}
                (test-data/rows+cols
                 (data/dataset test-data/countries
                   (data/run-mbql-query country)))))

    (is (match?
         {:data {:columns ["name" "db/id"],
                 :rows [["20th Century Cafe" pos-int?]
                        ["25°" pos-int?]
                        ["33 Taps" pos-int?]
                        ["800 Degrees Neapolitan Pizzeria" pos-int?]
                        ["BCD Tofu House" pos-int?]
                        ["Baby Blues BBQ" pos-int?]
                        ["Barney's Beanery" pos-int?]
                        ["Beachwood BBQ & Brewing" pos-int?]
                        ["Beyond Sushi" pos-int?]
                        ["Bludso's BBQ" pos-int?]]}}
         (data/run-mbql-query venues
           {:fields   [$name [:field-id (data/id :venues "db/id")]]
            :limit    10
            :order-by [[:asc $name]]})))))


(deftest aggregrate-count-test
  (with-datomic
    (is (match? {:data {:columns ["f1" "count"],
                        :rows    [["xxx" 2] ["yyy" 1]]}}
                (data/dataset test-data/aggr-data
                  (data/run-mbql-query foo
                    {:aggregation [[:count]]
                     :breakout    [$f1]}))))))

(deftest order-by-test
  (with-datomic
    (is (match? {:data {:columns ["db/id" "name" "code"],
                        :rows
                        [[pos-int? "Germany" "DE"]
                         [pos-int? "Finnland" "FI"]
                         [pos-int? "Belgium" "BE"]]}}
                (data/dataset test-data/countries
                  (data/run-mbql-query country
                    {:order-by [[:desc $name]]}))))))
