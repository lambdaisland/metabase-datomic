(ns metabase.driver.datomic.filter-test
  (:require [clojure.test :refer :all]
            [metabase.driver.datomic.test :refer :all]
            [metabase.test.data :as data]
            [metabase.test.util.timezone :as tu.tz]))

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

(comment






;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                            STRING SEARCH FILTERS - CONTAINS, STARTS-WITH, ENDS-WITH                            |
;;; +----------------------------------------------------------------------------------------------------------------+

;;; -------------------------------------------------- starts-with ---------------------------------------------------

  (expect-with-non-timeseries-dbs
   [[41 "Cheese Steak Shop" 18 37.7855 -122.44  1]
    [74 "Chez Jay"           2 34.0104 -118.493 2]]
   (-> (data/run-mbql-query venues
         {:filter   [:starts-with $name "Che"]
          :order-by [[:asc $id]]})
       rows formatted-venues-rows))

  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :case-sensitivity-string-filter-options)
                                []
                                (-> (data/run-mbql-query venues
                                      {:filter   [:starts-with $name "CHE"]
                                       :order-by [[:asc $id]]})
                                    rows formatted-venues-rows))

  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :case-sensitivity-string-filter-options)
                                [[41 "Cheese Steak Shop" 18 37.7855 -122.44  1]
                                 [74 "Chez Jay"           2 34.0104 -118.493 2]]
                                (-> (data/run-mbql-query venues
                                      {:filter   [:starts-with $name "CHE" {:case-sensitive false}]
                                       :order-by [[:asc $id]]})
                                    rows formatted-venues-rows))


;;; --------------------------------------------------- ends-with ----------------------------------------------------

  (expect-with-non-timeseries-dbs
   [[ 5 "Brite Spot Family Restaurant" 20 34.0778 -118.261 2]
    [ 7 "Don Day Korean Restaurant"    44 34.0689 -118.305 2]
    [17 "Ruen Pair Thai Restaurant"    71 34.1021 -118.306 2]
    [45 "Tu Lan Restaurant"             4 37.7821 -122.41  1]
    [55 "Dal Rae Restaurant"           67 33.983  -118.096 4]]
   (-> (data/run-mbql-query venues
         {:filter   [:ends-with $name "Restaurant"]
          :order-by [[:asc $id]]})
       rows formatted-venues-rows))

  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :case-sensitivity-string-filter-options)
                                []
                                (-> (data/run-mbql-query venues
                                      {:filter   [:ends-with $name "RESTAURANT"]
                                       :order-by [[:asc $id]]})
                                    rows formatted-venues-rows))

  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :case-sensitivity-string-filter-options)
                                [[ 5 "Brite Spot Family Restaurant" 20 34.0778 -118.261 2]
                                 [ 7 "Don Day Korean Restaurant"    44 34.0689 -118.305 2]
                                 [17 "Ruen Pair Thai Restaurant"    71 34.1021 -118.306 2]
                                 [45 "Tu Lan Restaurant"             4 37.7821 -122.41  1]
                                 [55 "Dal Rae Restaurant"           67 33.983  -118.096 4]]
                                (-> (data/run-mbql-query venues
                                      {:filter   [:ends-with $name "RESTAURANT" {:case-sensitive false}]
                                       :order-by [[:asc $id]]})
                                    rows formatted-venues-rows))

;;; ---------------------------------------------------- contains ----------------------------------------------------
  (expect-with-non-timeseries-dbs
   [[31 "Bludso's BBQ"             5 33.8894 -118.207 2]
    [34 "Beachwood BBQ & Brewing" 10 33.7701 -118.191 2]
    [39 "Baby Blues BBQ"           5 34.0003 -118.465 2]]
   (-> (data/run-mbql-query venues
         {:filter   [:contains $name "BBQ"]
          :order-by [[:asc $id]]})
       rows formatted-venues-rows))

  ;; case-insensitive
  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :case-sensitivity-string-filter-options)
                                []
                                (-> (data/run-mbql-query venues
                                      {:filter   [:contains $name "bbq"]
                                       :order-by [[:asc $id]]})
                                    rows formatted-venues-rows))

  ;; case-insensitive
  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :case-sensitivity-string-filter-options)
                                [[31 "Bludso's BBQ"             5 33.8894 -118.207 2]
                                 [34 "Beachwood BBQ & Brewing" 10 33.7701 -118.191 2]
                                 [39 "Baby Blues BBQ"           5 34.0003 -118.465 2]]
                                (-> (data/run-mbql-query venues
                                      {:filter   [:contains $name "bbq" {:case-sensitive false}]
                                       :order-by [[:asc $id]]})
                                    rows formatted-venues-rows))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             NESTED AND/OR CLAUSES                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

  (expect-with-non-timeseries-dbs
   [[81]]
   (->> (data/run-mbql-query venues
          {:aggregation [[:count]]
           :filter      [:and
                         [:!= $price 3]
                         [:or
                          [:= $price 1]
                          [:= $price 2]]]})
        rows (format-rows-by [int])))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         = AND != WITH MULTIPLE VALUES                                          |
;;; +----------------------------------------------------------------------------------------------------------------+

  (expect-with-non-timeseries-dbs
   [[81]]
   (->> (data/run-mbql-query venues
          {:aggregation [[:count]]
           :filter      [:= $price 1 2]})
        rows (format-rows-by [int])))

  (expect-with-non-timeseries-dbs
   [[19]]
   (->> (data/run-mbql-query venues
          {:aggregation [[:count]]
           :filter      [:!= $price 1 2]})
        rows (format-rows-by [int])))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                   NOT FILTER                                                   |
;;; +----------------------------------------------------------------------------------------------------------------+

  ;; `not` filter -- Test that we can negate the various other filter clauses
  ;;
  ;; The majority of these tests aren't necessary since `not` automatically translates them to simpler, logically
  ;; equivalent expressions but I already wrote them so in this case it doesn't hurt to have a little more test coverage
  ;; than we need
  ;;

;;; =
  (expect-with-non-timeseries-dbs
   [99]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:= $id 1]]}))))

;;; !=
  (expect-with-non-timeseries-dbs
   [1]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:!= $id 1]]}))))
;;; <
  (expect-with-non-timeseries-dbs
   [61]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:< $id 40]]}))))

;;; >
  (expect-with-non-timeseries-dbs
   [40]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:> $id 40]]}))))

;;; <=
  (expect-with-non-timeseries-dbs
   [60]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:<= $id 40]]}))))

;;; >=
  (expect-with-non-timeseries-dbs
   [39]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:>= $id 40]]}))))

;;; is-null
  (expect-with-non-timeseries-dbs
   [100]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:is-null $id]]}))))

;;; between
  (expect-with-non-timeseries-dbs
   [89]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:between $id 30 40]]}))))

;;; inside
  (expect-with-non-timeseries-dbs
   [39]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:inside $latitude $longitude 40 -120 30 -110]]}))))

;;; starts-with
  (expect-with-non-timeseries-dbs
   [80]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:starts-with $name "T"]]}))))

;;; contains
  (expect-with-non-timeseries-dbs
   [97]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:contains $name "BBQ"]]}))))

;;; does-not-contain
  ;;
  ;; This should literally be the exact same query as the one above by the time it leaves the Query eXpander, so this is
  ;; more of a QX test than anything else
  (expect-with-non-timeseries-dbs
   [97]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:does-not-contain $name "BBQ"]}))))

;;; ends-with
  (expect-with-non-timeseries-dbs
   [87]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:ends-with $name "a"]]}))))

;;; and
  (expect-with-non-timeseries-dbs
   [98]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:and
                                           [:> $id 32]
                                           [:contains $name "BBQ"]]]}))))
;;; or
  (expect-with-non-timeseries-dbs
   [31]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:or
                                           [:> $id 32]
                                           [:contains $name "BBQ"]]]}))))

;;; nested and/or
  (expect-with-non-timeseries-dbs
   [96]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:or
                                           [:and
                                            [:> $id 32]
                                            [:< $id 35]]
                                           [:contains $name "BBQ"]]]}))))

;;; nested not
  (expect-with-non-timeseries-dbs
   [3]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:not [:not [:contains $name "BBQ"]]]}))))

;;; not nested inside and/or
  (expect-with-non-timeseries-dbs
   [1]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query venues
                      {:aggregation [[:count]]
                       :filter      [:and
                                     [:not [:> $id 32]]
                                     [:contains $name "BBQ"]]}))))


  ;; make sure that filtering with dates truncating to minutes works (#4632)
  (expect-with-non-timeseries-dbs
   [107]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query checkins
                      {:aggregation [[:count]]
                       :filter      [:between [:datetime-field $date :minute] "2015-01-01T12:30:00" "2015-05-31"]}))))

  ;; make sure that filtering with dates bucketing by weeks works (#4956)
  (expect-with-non-timeseries-dbs
   [7]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query checkins
                      {:aggregation [[:count]]
                       :filter      [:= [:datetime-field $date :week] "2015-06-21T07:00:00.000000000-00:00"]}))))



;;; FILTER -- "INSIDE"
  (expect-with-non-timeseries-dbs
   [[1 "Red Medicine" 4 10.0646 -165.374 3]]
   (-> (data/run-mbql-query venues
         {:filter [:inside $latitude $longitude 10.0649 -165.379 10.0641 -165.371]})
       rows formatted-venues-rows))

;;; FILTER - `is-null` & `not-null` on datetime columns
  (expect-with-non-timeseries-dbs
   [1000]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query checkins
                      {:aggregation [[:count]]
                       :filter      [:not-null $date]}))))

  ;; Creates a query that uses a field-literal. Normally our test queries will use a field placeholder, but
  ;; https://github.com/metabase/metabase/issues/7381 is only triggered by a field literal
  (expect-with-non-timeseries-dbs
   [1000]
   (first-row
    (format-rows-by [int]
                    (data/run-mbql-query checkins
                      {:aggregation [[:count]]
                       :filter      ["NOT_NULL"
                                     ["field-id"
                                      ["field-literal" (data/format-name "date") "type/DateTime"]]]}))))

  (expect-with-non-timeseries-dbs
   true
   (let [result (first-row (data/run-mbql-query checkins
                             {:aggregation [[:count]]
                              :filter      [:is-null $date]}))]
     ;; Some DBs like Mongo don't return any results at all in this case, and there's no easy workaround
     (contains? #{[0] [0M] [nil] nil} result)))
  )
