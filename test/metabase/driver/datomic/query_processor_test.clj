(ns metabase.driver.datomic.query-processor-test
  (:require [metabase.driver.datomic.query-processor :as datomic.qp]
            [metabase.driver.datomic.test :refer :all]
            [metabase.driver.datomic.test-data :as test-data]
            [metabase.query-processor :as qp]
            [metabase.test.data :as data]
            [clojure.test :refer :all]))

(deftest select-field-test
  (let [select-name (datomic.qp/select-field
                     '{:find [?foo ?artist]}
                     {1 {:artist/name "abc"}}
                     '(:artist/name ?artist))]
    (is (= "abc"
           (select-name [2 1])))))

(deftest select-fields-test
  (let [select-fn (datomic.qp/select-fields
                   '{:find [?artist (count ?artist)]}
                   {1 {:artist/name "abc"}
                    7 {:artist/name "foo"}}
                   '[(count ?artist) (:artist/name ?artist)])]
    (is (= [42 "foo"]
           (select-fn [7 42])))))

(deftest execute-native-query-test
  (with-datomic
    (is (= {:columns ["?code" "?country"]
            :rows [["BE" "Belgium"]
                   ["FI" "Finnland"]
                   ["DE" "Germany"]]}
           (test-data/rows+cols
            (data/with-db-for-dataset [db test-data/countries]
              (qp/process-query
                {:type :native
                 :database (:id db)
                 :native
                 {:query
                  (pr-str '{:find [?code ?country]
                            :where [[?eid :country/code ?code]
                                    [?eid :country/name ?country]]})}})))))))
