(ns metabase.driver.datomic.aggregates-test
  (:require [clojure.test :refer :all]
            [metabase.driver.datomic.test :refer :all]
            [metabase.driver.datomic.test-data :as test-data]
            [metabase.test.data :as data]))

(deftest aggregrate-count-test
  (with-datomic
    (is (match? {:data {:columns ["f1" "count"],
                        :rows    [["xxx" 2] ["yyy" 1]]}}
                (data/dataset test-data/aggr-data
                  (data/run-mbql-query foo
                    {:aggregation [[:count]]
                     :breakout    [$f1]}))))))
