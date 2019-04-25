(ns metabase.driver.datomic.order-test
  (:require [clojure.test :refer :all]
            [metabase.driver.datomic.test :refer :all]
            [metabase.driver.datomic.test-data :as test-data]
            [metabase.test.data :as data]))

(deftest order-by-test
  (with-datomic
    (is (match? {:data {:columns ["db/id" "name" "code"],
                        :rows
                        [[pos-int? "Germany" "DE"]
                         [pos-int? "Finnland" "FI"]
                         [pos-int? "Belgium" "BE"]]}}
                (data/with-temp-db [_ test-data/countries]
                  (data/run-mbql-query country
                    {:order-by [[:desc $name]]}))))))
