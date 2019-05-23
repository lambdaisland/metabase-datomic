(ns metabase.driver.datomic.datomic-test
  "Checks for things that are particular to datomic, and regression tests."
  (:require [clojure.test :refer :all]
            [metabase.driver.datomic.test :refer :all]
            [metabase.driver.datomic.test-data :as test-data]
            [metabase.models.field :refer [Field]]
            [metabase.models.database :refer [Database]]
            [metabase.query-processor-test :refer [aggregate-col]]
            [metabase.test.data :as data]
            [metabase.test.util :as tu]
            [metabase.driver.datomic.fix-types :as fix-types]
            [toucan.db :as db]
            [datomic.api :as d]
            [metabase.sync :as sync]))

;; Skip these tests if the eeleven sample DB does not exist.
(try
  (d/connect "datomic:free://localhost:4334/eeleven")
  (catch Exception e
    (alter-meta! *ns* assoc :kaocha/skip true)))

(defn get-or-create-eeleven-db! []
  (if-let [db-inst (db/select-one Database :name "Eeleven")]
    db-inst
    (let [db (db/insert! Database
               :name    "Eeleven"
               :engine  :datomic
               :details {:db "datomic:free://localhost:4334/eeleven"})]
      (sync/sync-database! db)
      (fix-types/undo-invalid-primary-keys!)
      (Database (:id db)))))

(defmacro with-datomic-db [& body]
  `(with-datomic
     (data/with-db (get-or-create-eeleven-db!)
       ~@body)))

(deftest filter-cardinality-many
  (is (= [17592186046126]
         (->> (with-datomic-db
                (data/run-mbql-query ledger
                  {:filter [:= [:field-id $journal-entries] 17592186046126]
                   :fields [$journal-entries]}))
              :data
              :rows
              (map first)))))

;; check that idents are returned as such, and can be filtered
(deftest ident-test
  (is (match? {:data {:rows [[:currency/CAD] [:currency/HKD] [:currency/SGD]]}}
              (with-datomic-db
                (data/run-mbql-query journal-entry
                  {:fields [$currency]
                   :order-by [[:asc $currency]]}))))

  (is (match? {:data {:rows [[3]]}}
              (with-datomic-db
                (data/run-mbql-query journal-entry
                  {:aggregation [[:count]]
                   :filter [:= $currency "currency/HKD"]})))))

;; Do grouping across nil placeholders
;; {:find [(count ?foo) ?bar]}
;; where :bar can be "nil"
(deftest group-across-nil
  (is (match? {:data {:columns ["id" "count"]
                      :rows [["JE-1556-47-8585" 2]
                             ["JE-2117-58-6345" 3]
                             ["JE-5555-47-8584" 2]]}}
              (with-datomic-db
                (data/run-mbql-query journal-entry
                  {:aggregation [[:count $journal-entry-lines]]
                   :breakout [$id]
                   :filter [:= $currency "currency/HKD"]
                   :order-by [[:asc $id]]})))))

(deftest cum-sum-across-fk
  (is (match? {:row_count 48
               :data {:columns ["date:week" "sum"]}}
              (with-datomic-db
                (data/run-mbql-query journal-entry
                  {:breakout [[:datetime-field $date :week]]
                   :aggregation [[:cum-sum [:fk->
                                            $journal-entry-lines
                                            [:field-id (data/id "journal-entry-line" "amount")]]]]
                   :order-by [[:asc [:datetime-field $date :week]]]})))))

(deftest filter-on-foreign-key
  ;; specifically this checks that filtering on a foreign key works correctly
  ;; even if the foreign field is not returned in the result. In other words: it
  ;; checks that constant-binding works correctly for foreign fields.
  (is (match? {:data {:columns ["db/id" "id" "journal-entries" "name" "tax-entries"],
                      :rows [[17592186045737 "LGR-5487-92-0122" 17592186045912 "SG-01-GL-01" nil]]}},
              (with-datomic-db
                (data/run-mbql-query ledger
                  {:filter [:= [:fk-> $journal-entries (data/id "journal-entry" "id")] "JE-2879-00-0055"]})))))
