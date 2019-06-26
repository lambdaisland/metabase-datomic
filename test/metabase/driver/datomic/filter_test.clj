(ns metabase.driver.datomic.filter-test
  (:require [clojure.test :refer :all]
            [metabase.driver.datomic.test :refer :all]
            [metabase.test.data :as data]
            [metabase.test.util.timezone :as tu.tz]
            [metabase.api.dataset :as dataset]
            [metabase.driver.datomic.test-data :as test-data]))

(deftest and-gt-gte
  (is (match? {:data {:rows [[pos-int? "Sushi Nakazawa" pos-int? 40.7318 -74.0045 4]
                             [pos-int? "Sushi Yasuda" pos-int? 40.7514 -73.9736 4]
                             [pos-int? "Tanoshi Sushi & Sake Bar" pos-int? 40.7677 -73.9533 4]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:filter   [:and [:> $latitude 40] [:>= $price 4]]
                   :order-by [[:asc [:field-id (data/id "venues" "db/id")]]]})))))


(deftest and-lt-gt-ne
  (is (match? {:data {:rows
                      [[pos-int? "Red Medicine" pos-int? 10.0646 -165.374 3]
                       [pos-int? "Jones Hollywood" pos-int? 34.0908 -118.346 3]
                       [pos-int? "Boneyard Bistro" pos-int? 34.1477 -118.428 3]
                       [pos-int? "Marlowe" pos-int? 37.7767 -122.396 3]
                       [pos-int? "Hotel Biron" pos-int? 37.7735 -122.422 3]
                       [pos-int? "Empress of China" pos-int? 37.7949 -122.406 3]
                       [pos-int? "Tam O'Shanter" pos-int? 34.1254 -118.264 3]
                       [pos-int? "Yamashiro Hollywood" pos-int? 34.1057 -118.342 3]
                       [pos-int? "Musso & Frank Grill" pos-int? 34.1018 -118.335 3]
                       [pos-int? "Taylor's Prime Steak House" pos-int? 34.0579 -118.302 3]
                       [pos-int? "Pacific Dining Car" pos-int? 34.0555 -118.266 3]
                       [pos-int? "Polo Lounge" pos-int? 34.0815 -118.414 3]
                       [pos-int? "Blue Ribbon Sushi" pos-int? 40.7262 -74.0026 3]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:filter   [:and [:< $price 4] [:> $price 1] [:!= $price 2]]
                   :order-by [[:asc [:field-id (data/id "venues" "db/id")]]]})))))

(deftest filter-with-false-value
  ;; Check that we're checking for non-nil values, not just logically true ones.
  ;; There's only one place (out of 3) that I don't like
  (is (match? {:data {:rows [[1]]}}
              (with-datomic
                (data/dataset places-cam-likes
                  (data/run-mbql-query places
                    {:aggregation [[:count]]
                     :filter      [:= $liked false]}))))))

(deftest filter-equals-true
  (is (match? {:data {:rows [[pos-int? "Tempest" true]
                             [pos-int? "Bullit"  true]]}}
              (with-datomic
                (data/dataset places-cam-likes
                  (data/run-mbql-query places
                    {:filter      [:= $liked true]
                     :order-by [[:asc [:field-id (data/id "places" "db/id")]]]}))))))

(deftest filter-not-is-false
  (is (match? {:data {:rows [[pos-int? "Tempest" true]
                             [pos-int? "Bullit"  true]]}}
              (with-datomic
                (data/dataset places-cam-likes
                  (data/run-mbql-query places
                    {:filter      [:!= $liked false]
                     :order-by [[:asc [:field-id (data/id "places" "db/id")]]]}))))))

(deftest filter-not-is-true
  (is (match? {:data {:rows [[pos-int? "The Dentist" false]]}}
              (with-datomic
                (data/dataset places-cam-likes
                  (data/run-mbql-query places
                    {:filter      [:!= $liked true]
                     :order-by [[:asc [:field-id (data/id "places" "db/id")]]]}))))))

(deftest between-test
  (is (match? {:data {:rows [[pos-int? "Beachwood BBQ & Brewing" pos-int? 33.7701 -118.191 2]
                             [pos-int? "Bludso's BBQ" pos-int? 33.8894 -118.207 2]
                             [pos-int? "Dal Rae Restaurant" pos-int? 33.983 -118.096 4]
                             [pos-int? "Wurstküche" pos-int? 33.9997 -118.465 2]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:filter   [:between $latitude 33 34]
                   :order-by [[:asc $latitude]]})))))


(deftest between-dates
  (is (match? {:data {:rows [[29]]}}
              (with-datomic
                (tu.tz/with-jvm-tz "UTC"
                  (data/run-mbql-query checkins
                    {:aggregation [[:count]]
                     :filter [:between [:datetime-field $date :day] "2015-04-01" "2015-05-01"]}))))))

(deftest or-lte-eq
  (is (match?
       {:data {:rows [[pos-int? "Red Medicine" pos-int? 10.0646 -165.374 3]
                      [pos-int? "The Apple Pan" pos-int? 34.0406 -118.428 2]
                      [pos-int? "Bludso's BBQ" pos-int? 33.8894 -118.207 2]
                      [pos-int? "Beachwood BBQ & Brewing" pos-int? 33.7701 -118.191 2]]}}

       (with-datomic
         (data/run-mbql-query venues
           {:filter   [:or [:<= $latitude 33.9] [:= $name "The Apple Pan"]]
            :order-by [[:asc [:field-id (data/id "venues" "db/id")]]]})))))

(deftest starts-with
  (is (match? {:data {:columns ["name"],
                      :rows    []}}
              (with-datomic
                (data/run-mbql-query venues
                  {:fields   [$name]
                   :filter   [:starts-with $name "CHE"]
                   :order-by [[:asc [:field-id (data/id "venues" "db/id")]]]}))))

  (is (match? {:data {:columns ["name"],
                      :rows    [["Cheese Steak Shop"] ["Chez Jay"]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:fields   [$name]
                   :filter   [:starts-with $name "CHE" {:case-sensitive false}]
                   :order-by [[:asc [:field-id (data/id "venues" "db/id")]]]})))))

(deftest ends-with
  (is (match? {:data {:columns ["name" "latitude" "longitude" "price"]
                      :rows    [["Brite Spot Family Restaurant" 34.0778 -118.261 2]
                                ["Don Day Korean Restaurant"    34.0689 -118.305 2]
                                ["Ruen Pair Thai Restaurant"    34.1021 -118.306 2]
                                ["Tu Lan Restaurant"            37.7821 -122.41  1]
                                ["Dal Rae Restaurant"           33.983  -118.096 4]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:fields   [$name $latitude $longitude $price]
                   :filter   [:ends-with $name "Restaurant"]
                   :order-by [[:asc (data/id "venues" "db/id")]]}))))

  (is (match? {:data {:columns ["name" "latitude" "longitude" "price"]
                      :rows    []}}
              (with-datomic
                (data/run-mbql-query venues
                  {:fields   [$name $latitude $longitude $price]
                   :filter   [:ends-with $name "RESTAURANT" {:case-sensitive true}]
                   :order-by [[:asc (data/id "venues" "db/id")]]}))))

  (is (match? {:data {:columns ["name" "latitude" "longitude" "price"]
                      :rows    [["Brite Spot Family Restaurant" 34.0778 -118.261 2]
                                ["Don Day Korean Restaurant"    34.0689 -118.305 2]
                                ["Ruen Pair Thai Restaurant"    34.1021 -118.306 2]
                                ["Tu Lan Restaurant"            37.7821 -122.41  1]
                                ["Dal Rae Restaurant"           33.983  -118.096 4]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:fields   [$name $latitude $longitude $price]
                   :filter   [:ends-with $name "RESTAURANT" {:case-sensitive false}]
                   :order-by [[:asc (data/id "venues" "db/id")]]})))))

(deftest contains-test
  (is (match? {:data {:rows []}}
              (with-datomic
                (data/run-mbql-query venues
                  {:filter   [:contains $name "bbq"]
                   :order-by [[:asc (data/id "venues" "db/id")]]}))))

  (is (match? {:data {:rows [["Bludso's BBQ" 2]
                             ["Beachwood BBQ & Brewing" 2]
                             ["Baby Blues BBQ" 2]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:fields   [$name $price]
                   :filter   [:contains $name "BBQ"]
                   :order-by [[:asc (data/id "venues" "db/id")]]}))))

  (is (match? {:data {:rows [["Bludso's BBQ" 2]
                             ["Beachwood BBQ & Brewing" 2]
                             ["Baby Blues BBQ" 2]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:fields   [$name $price]
                   :filter   [:contains $name "bbq" {:case-sensitive false}]
                   :order-by [[:asc (data/id "venues" "db/id")]]})))))

(deftest nestend-and-or
  (is (match? {:data {:rows [[81]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:and
                                 [:!= $price 3]
                                 [:or
                                  [:= $price 1]
                                  [:= $price 2]]]})))))

(deftest =-!=-multiple-values
  (is (match? {:data {:rows [[81]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:= $price 1 2]}))))

  (is (match? {:data {:rows [[19]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:!= $price 1 2]})))))

(deftest not-filter
  (is (match? {:data {:rows [[41]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:not [:= $price 2]]}))))

  (is (match? {:data {:rows [[59]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:not [:!= $price 2]]}))))

  (is (match? {:data {:rows [[78]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:not [:< $price 2]]}))))

  (is (match? {:data {:rows [[81]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:not [:> $price 2]]}))))

  (is (match? {:data {:rows [[19]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:not [:<= $price 2]]}))))

  (is (match? {:data {:rows [[22]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:not [:>= $price 2]]}))))

  (is (match? {:data {:rows [[22]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:not [:between $price 2 4]]}))))

  (is (match? {:data {:rows [[80]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:not [:starts-with $name "T"]]}))))

  (is (match? {:data {:rows [[3]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:not [:not [:contains $name "BBQ"]]]}))))

  (is (match? {:data {:rows [[97]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:not [:contains $name "BBQ"]]}))))

  (is (match? {:data {:rows [[97]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:does-not-contain $name "BBQ"]}))))

  (is (match? {:data {:rows [[3]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter [:and
                            [:not [:> $price 2]]
                            [:contains $name "BBQ"]]}))))

  (is (match? {:data {:rows [[87]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:not [:ends-with $name "a"]]}))))

  (is (match? {:data {:rows [[84]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter [:not [:and
                                  [:contains $name "ar"]
                                  [:< $price 4]]]}))))

  (is (match? {:data {:rows [[4]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter [:not [:or
                                  [:contains $name "ar"]
                                  [:< $price 4]]]}))))

  (is (match? {:data {:rows [[24]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter [:not [:or
                                  [:contains $name "ar"]
                                  [:and
                                   [:> $price 1]
                                   [:< $price 4]]]]})))))

(deftest is-null
  (is (match? {:data {:rows [["DE" nil]
                             ["FI" nil]]}}
              (with-datomic
                (data/with-temp-db [_ test-data/with-nulls]
                  (data/run-mbql-query country
                    {:fields [$code $population]
                     :filter [:is-null $population]
                     :order-by [[:asc $code]]})))))

  (is (match? {:data {:rows [["BE" 11000000]]}}
              (with-datomic
                (data/with-temp-db [_ test-data/with-nulls]
                  (data/run-mbql-query country
                    {:fields [$code $population]
                     :filter [:not [:is-null $population]]}))))))

(deftest inside
  ;; This is converted to [:and [:between ..] [:between ..]] so we get this one for free
  (is (match? {:data {:rows [["Red Medicine" 10.0646 -165.374 3]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:fields [$name $latitude $longitude $price]
                   :filter [:inside $latitude $longitude 10.0649 -165.379 10.0641 -165.371]}))))

  (is (match? {:data {:rows [[39]]}}
              (with-datomic
                (data/run-mbql-query venues
                  {:aggregation [[:count]]
                   :filter      [:not [:inside $latitude $longitude 40 -120 30 -110]]})))))


;; These are related to historical bugs in Metabase itself, we simply copied
;; these over to make sure we can handle these cases.
(deftest edge-cases
  ;; make sure that filtering with dates truncating to minutes works (#4632)
  (is (match? {:data {:rows [[107]]}}
              (with-datomic
                (data/run-mbql-query checkins
                  {:aggregation [[:count]]
                   :filter      [:between [:datetime-field $date :minute] "2015-01-01T12:30:00" "2015-05-31"]}))))

  ;; make sure that filtering with dates bucketing by weeks works (#4956)
  (is (match? {:data {:rows [[7]]}}
              (with-datomic
                (tu.tz/with-jvm-tz "UTC"
                  (data/run-mbql-query checkins
                    {:aggregation [[:count]]
                     :filter      [:= [:datetime-field $date :week] "2015-06-21T07:00:00.000000000-00:00"]})))))

  ;; FILTER - `is-null` & `not-null` on datetime columns
  (is (match? {:data {:rows [[1000]]}}
              (with-datomic
                (data/run-mbql-query checkins
                  {:aggregation [[:count]]
                   :filter      [:not-null $date]}))))

  (is (match? {:data {:rows [[1000]]}}
              (with-datomic
                (data/run-mbql-query checkins
                  {:aggregation [[:count]]
                   :filter      ["NOT_NULL"
                                 ["field-id"
                                  ["field-literal" (data/format-name "date") "type/DateTime"]]]}))))

  (is (match? {:data {:rows [[0]]}}
              (with-datomic
                (data/run-mbql-query checkins
                  {:aggregation [[:count]]
                   :filter      [:is-null $date]})))))
