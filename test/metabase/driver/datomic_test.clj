(ns metabase.driver.datomic-test
  (:require [clojure.test :as test :refer [is are testing]]
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

(defmacro deftest [name & body]
  `(test/deftest ~name
     (driver/with-driver :datomic
       ~@body)))

(deftest basic-query-test
  (is (match? {:columns ["db/id" "name" "code"]
               :rows    [[pos-int? "Belgium" "BE"]
                         [pos-int? "Germany" "DE"]
                         [pos-int? "Finnland" "FI"]]}
              (test-data/rows+cols
               (data/dataset test-data/countries
                 (data/run-mbql-query country))))))

(comment
  (driver/with-driver :datomic
    (test-data/rows+cols
     (data/dataset test-data/aggr-data
       (data/run-mbql-query foo
         {:aggregation [[:count]]
          :breakout    [$f1]}))))

  (nqry)

  (driver/with-driver :datomic
    (data/dataset test-data/countries
      (data/run-mbql-query country)))


  (user/refer-repl)
  (remove-database "countries")


  (driver/with-driver :datomic
    (data/dataset test-data/countries
      (data/run-mbql-query country)))

  (db/select Database)

  (user/setup-driver!))

(deftest aggregrate-count-test
  #_(is (= {:columns ["F1" "count"],
            :rows    [["xxx" 2] ["yyy" 1]]}))
  (prn (data/dataset test-data/aggr-data
         (data/run-mbql-query foo
           {:aggregation [[:count]]
            :breakout    [$f1]})))

  ;; Currently we turn this
  {:source-table 39,
   :breakout [[:field-id 158]],
   :aggregation [[:count]],
   :order-by [[:asc [:field-id 158]]]}

  ;; Into this
  '{:where [(or [?eid :foo/f1] [?eid :foo/f2]) [?eid :foo/f1 ?breakout-0]],
    :order-by [[:asc [:entity 0 :foo/f1]]],
    :find [?breakout-0 (count ?eid)],
    :fields [[:nth 0] [:nth 1]]}

  ;; This is problematic because the first column in the result isn't an entity
  ;; id, so [:entity 0 :foo/f1] fails. This is because [:field-id 158] is
  ;; handled completely differently if it's in :fields vs :breakout. maybe we
  ;; should stick the eid into the result together with the broken out fields?

  '{:where [(or [?eid :foo/f1] [?eid :foo/f2]) [?eid :foo/f1 ?breakout-0]], :order-by [[:asc [:entity 0 :foo/f1]]], :find [?breakout-0 (count ?eid)], :fields [[:nth 0] [:nth 1]]}


  )

(deftest order-by-test
  (is (match? {:columns ["db/id" "name" "code"],
               :rows
               [[pos-int? "Germany" "DE"]
                [pos-int? "Finnland" "FI"]
                [pos-int? "Belgium" "BE"]]}
              (test-data/rows+cols
               (data/dataset test-data/countries
                 (data/run-mbql-query country
                   {:order-by [[:desc $name]]}))))))
