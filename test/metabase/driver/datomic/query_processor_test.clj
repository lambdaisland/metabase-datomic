(ns metabase.driver.datomic.query-processor-test
  (:require [metabase.driver.datomic.query-processor :as datomic.qp]
            [metabase.query-processor :as qp]
            [metabase.driver.datomic.test :refer :all]
            [metabase.test.data :as data]
            [toucan.db :as db]
            [metabase.models.database :as database]
            [metabase.driver.datomic.test-data :as test-data]
            [metabase.driver :as driver]))

(deftest execute-native-query-test
  (is (= {:columns ["?code" "?country"]
          :rows [["BE" "Belgium"]
                 ["FI" "Finnland"]
                 ["DE" "Germany"]]}
         (test-data/rows+cols
          (data/with-temp-db [db test-data/countries]
            (qp/process-query
              {:type :native
               :database (:id db)
               :native
               {:query
                (pr-str '{:find [?code ?country]
                          :where [[?eid :country/code ?code]
                                  [?eid :country/name ?country]]})}}))))))
