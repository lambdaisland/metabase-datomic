(ns metabase.driver.datomic.aggregation-test
  (:require [clojure.test :refer :all]
            [metabase.driver.datomic.test :refer :all]
            [metabase.driver.datomic.test-data :as test-data]
            [metabase.models.field :refer [Field]]
            [metabase.query-processor-test :refer [aggregate-col]]
            [metabase.test.data :as data]
            [metabase.test.util :as tu]))

(deftest count-test
  (is (match? {:data {:columns ["f1" "count"],
                      :rows    [["xxx" 2] ["yyy" 1]]}}
              (with-datomic
                (data/with-temp-db [_ test-data/aggr-data]
                  (data/run-mbql-query foo
                    {:aggregation [[:count]]
                     :breakout    [$f1]})))))

  (is (match? {:data {:columns ["count"]
                      :rows [[100]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]})))))

(deftest sum-test
  (is (match? {:data {:columns ["sum"]
                      :rows [[203]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:sum $price]]})))))

(deftest avg-test
  (is (match? {:data {:columns ["avg"]
                      :rows [[#(= 355058 (long (* 10000 %)))]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:avg $latitude]]})))))

(deftest distinct-test
  (is (match? {:data {:columns ["count"]
                      :rows [[15]]}}
              (with-datomic
                (data/run-mbql-query checkins
                  {:aggregation [[:distinct $user_id]]})))))

(deftest no-aggregation-test
  (is (match? {:data
               {:rows
                [[pos-int? "Red Medicine"                 pos-int? 10.0646 -165.374 3]
                 [pos-int? "Stout Burgers & Beers"        pos-int? 34.0996 -118.329 2]
                 [pos-int? "The Apple Pan"                pos-int? 34.0406 -118.428 2]
                 [pos-int? "Wurstküche"                   pos-int? 33.9997 -118.465 2]
                 [pos-int? "Brite Spot Family Restaurant" pos-int? 34.0778 -118.261 2]
                 [pos-int? "The 101 Coffee Shop"          pos-int? 34.1054 -118.324 2]
                 [pos-int? "Don Day Korean Restaurant"    pos-int? 34.0689 -118.305 2]
                 [pos-int? "25°"                          pos-int? 34.1015 -118.342 2]
                 [pos-int? "Krua Siri"                    pos-int? 34.1018 -118.301 1]
                 [pos-int? "Fred 62"                      pos-int? 34.1046 -118.292 2]]}}

              (with-datomic
                (data/run-mbql-query venues
                  {:limit    10
                   :order-by [[:asc (data/id "venues" "db/id")]]})))))

(deftest stddev-test
  (is (match? {:data {:columns ["stddev"]
                      :rows [[#(= 3417 (long (* 1000 %)))]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:stddev $latitude]]})))))

(deftest min-test
  (is (match? {:data {:columns ["min"]
                      :rows [[1]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:min $price]]}))))

  (is (match? {:data
               {:columns ["price" "min"],
                :rows [[1 34.0071] [2 33.7701] [3 10.0646] [4 33.983]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:min $latitude]]
                   :breakout    [$price]})))))

(deftest max-test
  (is (match? {:data {:columns ["max"]
                      :rows [[4]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:max $price]]}))))

  (is (match? {:data {:columns ["price" "max"]
                      :rows [[1 37.8078] [2 40.7794] [3 40.7262] [4 40.7677]] }}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:max $latitude]]
                   :breakout    [$price]})))))

(deftest multiple-aggregates-test
  (testing "two aggregations"
    (is (match? {:data {:columns ["count" "sum"]
                        :rows [[100 203]]}}
                (with-datomic
                  (data/run-mbql-query venues
                    {:aggregation [[:count] [:sum $price]]})))))

  (testing "three aggregations"
    (is (match? {:data {:columns ["avg" "count" "sum"]
                        :rows [[2.03 100 203]]}}
                (with-datomic
                  (data/run-mbql-query venues
                    {:aggregation [[:avg $price] [:count] [:sum $price]]})))))

  (testing "second aggregate has correct metadata"
    (is (match? {:data
                 {:cols [(aggregate-col :count)
                         (assoc (aggregate-col :count) :name "count_2")]}}
                (with-datomic
                  (data/run-mbql-query venues
                    {:aggregation [[:count] [:count]]}))))))

(deftest edge-case-tests
  ;; Copied from the main metabase test base, these seem to have been added as
  ;; regression tests for specific issues. Can't hurt to run them for Datomic as
  ;; well.
  (testing "Field.settings show up for aggregate fields"
    (is (= {:base_type    :type/Integer
            :special_type :type/Category
            :settings     {:is_priceless false}
            :name         "sum"
            :display_name "sum"
            :source       :aggregation}
           (with-datomic
             (tu/with-temp-vals-in-db
                 Field (data/id :venues :price) {:settings {:is_priceless false}}
               (let [results (data/run-mbql-query venues
                               {:aggregation [[:sum [:field-id $price]]]})]
                 (or (-> results :data :cols first)
                     results)))))))

  (testing "handle queries that have more than one of the same aggregation?"
    (is (match? {:data {:rows [[#(< 3550.58 % 3550.59) 203]]}}
                (with-datomic
                  (data/run-mbql-query venues
                    {:aggregation [[:sum $latitude] [:sum $price]]}))))))

;; NOT YET IMPLEMENTED: cumulative count / cumulative sum
(comment
;;; Simple cumulative sum where breakout field is same as cum_sum field
  (qp-expect-with-all-drivers
   {:rows        [[ 1   1]
                  [ 2   3]
                  [ 3   6]
                  [ 4  10]
                  [ 5  15]
                  [ 6  21]
                  [ 7  28]
                  [ 8  36]
                  [ 9  45]
                  [10  55]
                  [11  66]
                  [12  78]
                  [13  91]
                  [14 105]
                  [15 120]]
    :columns     [(data/format-name "id")
                  "sum"]
    :cols        [(breakout-col (users-col :id))
                  (aggregate-col :sum (users-col :id))]
    :native_form true}
   (->> (data/run-mbql-query users
          {:aggregation [[:cum-sum $id]]
           :breakout [$id]})
        booleanize-native-form
        (format-rows-by [int int])))


;;; Cumulative sum w/ a different breakout field
  (qp-expect-with-all-drivers
   {:rows        [["Broen Olujimi"        14]
                  ["Conchúr Tihomir"      21]
                  ["Dwight Gresham"       34]
                  ["Felipinho Asklepios"  36]
                  ["Frans Hevel"          46]
                  ["Kaneonuskatew Eiran"  49]
                  ["Kfir Caj"             61]
                  ["Nils Gotam"           70]
                  ["Plato Yeshua"         71]
                  ["Quentin Sören"        76]
                  ["Rüstem Hebel"         91]
                  ["Shad Ferdynand"       97]
                  ["Simcha Yan"          101]
                  ["Spiros Teofil"       112]
                  ["Szymon Theutrich"    120]]
    :columns     [(data/format-name "name")
                  "sum"]
    :cols        [(breakout-col (users-col :name))
                  (aggregate-col :sum (users-col :id))]
    :native_form true}
   (->> (data/run-mbql-query users
          {:aggregation [[:cum-sum $id]]
           :breakout    [$name]})
        booleanize-native-form
        (format-rows-by [str int])
        tu/round-fingerprint-cols))


;;; Cumulative sum w/ a different breakout field that requires grouping
  (qp-expect-with-all-drivers
   {:columns     [(data/format-name "price")
                  "sum"]
    :cols        [(breakout-col (venues-col :price))
                  (aggregate-col :sum (venues-col :id))]
    :rows        [[1 1211]
                  [2 4066]
                  [3 4681]
                  [4 5050]]
    :native_form true}
   (->> (data/run-mbql-query venues
          {:aggregation [[:cum-sum $id]]
           :breakout    [$price]})
        booleanize-native-form
        (format-rows-by [int int])
        tu/round-fingerprint-cols))


;;; ------------------------------------------------ CUMULATIVE COUNT ------------------------------------------------

  (defn- cumulative-count-col [col-fn col-name]
    (assoc (aggregate-col :count (col-fn col-name))
      :base_type    :type/Integer
      :special_type :type/Number))

;;; cum_count w/o breakout should be treated the same as count
  (qp-expect-with-all-drivers
   {:rows        [[15]]
    :columns     ["count"]
    :cols        [(cumulative-count-col users-col :id)]
    :native_form true}
   (->> (data/run-mbql-query users
          {:aggregation [[:cum-count $id]]})
        booleanize-native-form
        (format-rows-by [int])))

;;; Cumulative count w/ a different breakout field
  (qp-expect-with-all-drivers
   {:rows        [["Broen Olujimi"        1]
                  ["Conchúr Tihomir"      2]
                  ["Dwight Gresham"       3]
                  ["Felipinho Asklepios"  4]
                  ["Frans Hevel"          5]
                  ["Kaneonuskatew Eiran"  6]
                  ["Kfir Caj"             7]
                  ["Nils Gotam"           8]
                  ["Plato Yeshua"         9]
                  ["Quentin Sören"       10]
                  ["Rüstem Hebel"        11]
                  ["Shad Ferdynand"      12]
                  ["Simcha Yan"          13]
                  ["Spiros Teofil"       14]
                  ["Szymon Theutrich"    15]]
    :columns     [(data/format-name "name")
                  "count"]
    :cols        [(breakout-col (users-col :name))
                  (cumulative-count-col users-col :id)]
    :native_form true}
   (->> (data/run-mbql-query users
          {:aggregation [[:cum-count $id]]
           :breakout    [$name]})
        booleanize-native-form
        (format-rows-by [str int])
        tu/round-fingerprint-cols))


  ;; Cumulative count w/ a different breakout field that requires grouping
  (qp-expect-with-all-drivers
   {:columns     [(data/format-name "price")
                  "count"]
    :cols        [(breakout-col (venues-col :price))
                  (cumulative-count-col venues-col :id)]
    :rows        [[1 22]
                  [2 81]
                  [3 94]
                  [4 100]]
    :native_form true}
   (->> (data/run-mbql-query venues
          {:aggregation [[:cum-count $id]]
           :breakout    [$price]})
        booleanize-native-form
        (format-rows-by [int int])
        tu/round-fingerprint-cols)))
