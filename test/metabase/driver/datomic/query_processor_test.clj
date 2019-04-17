(ns metabase.driver.datomic.query-processor-test
  (:require [metabase.driver.datomic.query-processor :as datomic.qp]
            [metabase.query-processor :as qp]
            [clojure.test :refer :all]
            [metabase.driver.datomic.test :refer :all]
            [metabase.test.data :as data]
            [toucan.db :as db]
            [metabase.models.database :as database]
            [metabase.driver.datomic.test-data :as test-data]
            [metabase.driver :as driver]
            [metabase.query-processor.store :as qp.store]
            [metabase.models.database :as database :refer [Database ->DatabaseInstance]]
            [metabase.models.table :as table :refer [Table ->TableInstance]]
            [metabase.driver.datomic.test :refer [with-datomic]]))

(def query->native (comp read-string :query qp/query->native))

#_(kaocha.repl/run)
(deftest fields-test
  (with-datomic
    (is (= '{:find     [?venues]
             :where
             [(or [?venues :venues/category_id]
                  [?venues :venues/latitude]
                  [?venues :venues/longitude]
                  [?venues :venues/name]
                  [?venues :venues/price])]
             :select   [(:venues/name ?venues) (:db/id ?venues)]
             :order-by [[:asc (:db/id ?venues)]]}

           (query->native
            (data/mbql-query venues
              {:fields   [$name [:field-id (data/id :venues "db/id")]]
               :limit    10
               :order-by [[:asc [:field-id (data/id :venues "db/id")]]]}))))))

(deftest breakout-test
  (with-datomic
    (is (= '{:find     [?checkins|checkins|user_id
                        (count ?checkins|checkins|user_id)]
             :where    [(or
                         [?checkins :checkins/date]
                         [?checkins :checkins/user_id]
                         [?checkins :checkins/venue_id])
                        [?checkins :checkins/user_id ?checkins|checkins|user_id]]
             :order-by [[:asc (:checkins/user_id ?checkins)]]
             :with     [?checkins]
             :select   [(:checkins/user_id ?checkins)
                        (count ?checkins|checkins|user_id)]}

           (query->native
            (data/mbql-query checkins
              {:aggregation [[:count]]
               :breakout    [$user_id]
               :order-by    [[:asc $user_id]]}))))

    (is (match? {:data {:rows    [[1 31] [2 70] [3 75] [4 77] [5 69]
                                  [6 70] [7 76] [8 81] [9 68] [10 78]
                                  [11 74] [12 59] [13 76] [14 62] [15 34]]
                        :columns ["user_id"
                                  "count"]}}

                (data/run-mbql-query checkins
                  {:aggregation [[:count]]
                   :breakout    [$user_id]
                   :order-by    [[:asc $user_id]]})))))

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

(with-datomic
  (query->native
   (data/mbql-query users
     {:breakout [[:datetime-field $last_login :hour]]
      :aggregation [[:count]]
      :order-by [[:asc [:datetime-field $last_login :hour]]]})))

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
