(ns metabase.driver.datomic.order-by-test
  (:require [clojure.test :refer :all]
            [metabase.driver :as driver]
            [metabase.driver.datomic.test :refer :all]
            [metabase.driver.datomic.test-data :as test-data]
            [metabase.test.data :as data]))

(def transpose (partial apply map vector))

(defn ranks
  "Convert a sequence of values into a sequence of 'ranks', a rank in this case
  being the position that each value has in the sorted input sequence."
  [xs]
  (let [idxs (into {} (reverse (map vector (sort xs) (next (range)))))]
    (mapv idxs xs)))

(defn mranks
  "Like ranks but applied to a matrix, where each element is replaced by its rank
  in its row.

  When sorting on id fields we don't know the exact values to be returned, since
  Datomic assigns the ids, but we know their relative ordering, i.e. rank. "
  [rows]
  (->> rows transpose (map ranks) transpose))

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

(deftest order-by-foreign-keys
  (is (= [[1 8 5]
          [1 4 3]
          [1 1 1]
          [4 10 2]
          [4 8 7]
          [4 7 4]
          [4 4 8]
          [4 4 10]
          [4 3 6]
          [4 2 9]]

         (with-datomic
           (-> (data/run-mbql-query checkins
                 {:fields   [$venue_id $user_id [:field-id (data/id "checkins" "db/id")]]
                  :order-by [[:asc $venue_id]
                             [:desc $user_id]
                             [:asc [:field-id (data/id "checkins" "db/id")]]]
                  :limit    10})
               (get-in [:data :rows])
               mranks)))))

(deftest order-by-aggregate-count
  (is (match? {:data {:columns ["price" "count"]
                      :rows [[4 6] [3 13] [1 22] [2 59]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :breakout    [$price]
                   :order-by    [[:asc [:aggregation 0]]]})))))

(deftest order-by-aggregate-sum
  (is (match? {:data {:columns ["price" "sum"]
                      :rows
                      [[2 2102.5237999999995]
                       [1 786.8249000000002]
                       [3 436.9022]
                       [4 224.33829999999998]]}}

              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:sum $latitude]]
                   :breakout    [$price]
                   :order-by    [[:desc [:aggregation 0]]]})))))

(deftest order-by-distinct
  (is (match? {:data {:columns ["price" "count"]
                      :rows [[4 6] [3 13] [1 22] [2 59]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:distinct [:field-id (data/id "venues" "db/id")]]]
                   :breakout    [$price]
                   :order-by    [[:asc [:aggregation 0]]]})))))

(deftest order-by-avg
  (is (match? {:data {:columns ["price" "avg"]
                      :rows
                      [[3 33.607861538461535]
                       [2 35.635996610169485]
                       [1 35.76476818181819]
                       [4 37.389716666666665]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:avg $latitude]]
                   :breakout    [$price]
                   :order-by    [[:asc [:aggregation 0]]]})))))

(deftest order-by-stddev-test
  (is (match?
       {:data {:columns ["price" "stddev"]
               :rows [[3 25.16407695964168]
                      [1 23.55773642142646]
                      [2 21.23640212816769]
                      [4 13.5]]}}

       (with-datomic
         (data/run-mbql-query venues
           {:aggregation [[:stddev $category_id]]
            :breakout    [$price]
            :order-by    [[:desc [:aggregation 0]]]})))))
