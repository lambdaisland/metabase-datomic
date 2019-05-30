(ns metabase.driver.datomic.source-query-test
  (:require [clojure.test :refer :all]
            [metabase.driver.datomic.test :refer :all]
            [metabase.driver.datomic.test-data :as test-data]
            [metabase.models.table :refer [Table]]
            [metabase.query-processor :as qp]
            [metabase.test.data :as data]
            [metabase.test.util.timezone :as tu.tz]
            [toucan.db :as db]))

(defn table->db [t]
  (db/select-one-field :db_id Table, :id (data/id t)))

(deftest only-source-query
  (is (match? {:data {:columns ["db/id" "name" "code"],
                      :rows
                      [[17592186045418 "Belgium" "BE"]
                       [17592186045420 "Finnland" "FI"]
                       [17592186045419 "Germany" "DE"]]}}

              (with-datomic
                (data/with-db-for-dataset [_ test-data/countries]
                  (qp/process-query
                    {:database (table->db "country")
                     :type     :query
                     :query    {:source-query {:source-table (data/id "country")
                                               :order-by [[:asc [:field-id (data/id "country" "name")]]]}}}))))))

(deftest source-query+aggregation
  (is (match? {:data {:columns ["price" "count"]
                      :rows [[1 22] [2 59] [3 13] [4 6]]}}
              (with-datomic
                (qp/process-query
                  (data/dataset test-data
                    {:database (table->db "venues")
                     :type     :query
                     :query    {:aggregation  [[:count]]
                                :breakout     [[:field-literal "price" :type/Integer]]
                                :source-query {:source-table (data/id :venues)}}}))))))


(deftest native-source-query+aggregation
  (is (match? {:data {:columns ["price" "count"]
                      :rows [[1 22] [2 59] [3 13] [4 6]]}}
              (with-datomic
                (qp/process-query
                  (data/dataset test-data
                    {:database (table->db "venues")
                     :type     :query
                     :query    {:aggregation  [[:count [:field-literal "venue" :type/Integer]]]
                                :breakout     [[:field-literal "price" :type/Integer]]
                                :source-query {:native
                                               (pr-str
                                                '{:find [?price]
                                                  :where [[?venue :venues/price ?price]]})}}}))))))


(deftest filter-source-query
  (is (match? {:data {:columns ["date"],
                      :rows
                      [["2015-01-04T08:00:00.000Z"]
                       ["2015-01-14T08:00:00.000Z"]
                       ["2015-01-15T08:00:00.000Z"]]}}
              (with-datomic
                (tu.tz/with-jvm-tz "UTC"
                  (qp/process-query
                    (data/dataset test-data
                      {:database (table->db "checkins")
                       :type     :query
                       :query    {:filter       [:between [:field-literal "date" :type/Date] "2015-01-01" "2015-01-15"]
                                  :order-by     [[:asc [:field-literal "date" :type/Date]]]
                                  :source-query {:fields [[:field-id (data/id "checkins" "date")]]
                                                 :source-table (data/id :checkins)}}})))))))

(deftest nested-source-query
  (is (match? {:row_count 25
               :data {:columns ["db/id" "name" "category_id" "latitude" "longitude" "price"]}}
              (with-datomic
                (qp/process-query
                  (data/dataset test-data
                    {:database (table->db "venues")
                     :type     :query
                     :query    {:limit        25
                                :source-query {:limit        50
                                               :source-query {:source-table (data/id :venues)
                                                              :limit        100}}}   }))))))
