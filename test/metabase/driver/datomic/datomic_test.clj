(ns metabase.driver.datomic.datomic-test
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

(defn get-or-create-eeleven-db! []
  (if-let [db-inst (db/select-one Database :name "Eeleven")]
    db-inst
    (do
      ;; explode if the Datomic DB does not exist
      (d/connect "datomic:free://localhost:4334/eeleven")
      (let [db (db/insert! Database
                 :name    "Eeleven"
                 :engine  :datomic
                 :details {:db "datomic:free://localhost:4334/eeleven"})]
        (sync/sync-database! db)
        (fix-types/undo-invalid-primary-keys!)
        (Database (:id db))))))

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


;; Do grouping across nil placeholders for various types
;; {:find [(count ?foo) ?bar]}
;; where :bar can be "nil"


;; check that idents are returned as such
