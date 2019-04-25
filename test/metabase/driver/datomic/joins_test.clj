(ns metabase.driver.datomic.joins-test
  (:require [clojure.test :refer :all]
            [metabase.driver.datomic.test :refer [with-datomic]]
            [metabase.models.database :refer [Database]]
            [metabase.test.data :as data]
            [toucan.db :as db]))

(deftest breakout-fk-test
  (testing "breakout FK Field is returned in the results"
    ;; The top 10 cities by number of Tupac sightings
    (is (match? {:data {:columns ["name" "count"]
                        :rows [["Arlington"    16]
                               ["Albany"       15]
                               ["Portland"     14]
                               ["Louisville"   13]
                               ["Philadelphia" 13]
                               ["Anchorage"    12]
                               ["Lincoln"      12]
                               ["Houston"      11]
                               ["Irvine"       11]
                               ["Lakeland"     11]]}}
                (with-datomic
                  (data/dataset tupac-sightings
                    (data/run-mbql-query sightings
                      {:aggregation [[:count]]
                       :breakout    [$city_id->cities.name]
                       :order-by    [[:desc [:aggregation 0]]]
                       :limit       10})))))))

(deftest filter-on-fk-field-test
  ;; Number of Tupac sightings in the Expa office
  (testing "we can filter on an FK field"
    (is (match? {:data {:columns ["count"]
                        :rows [[60]]}}
                (with-datomic
                  (data/dataset tupac-sightings
                    (data/run-mbql-query sightings
                      {:aggregation [[:count]]
                       :filter      [:= $category_id->categories.name "In the Expa Office"]})))))))

(deftest fk-field-in-fields-clause-test
  ;; THE 10 MOST RECENT TUPAC SIGHTINGS (!)
  (testing "we can include the FK field in the fields clause"
    (is (match? {:data {:columns ["name"]
                        :rows [["In the Park"]
                               ["Working at a Pet Store"]
                               ["At the Airport"]
                               ["At a Restaurant"]
                               ["Working as a Limo Driver"]
                               ["At Starbucks"]
                               ["On TV"]
                               ["At a Restaurant"]
                               ["Wearing a Biggie Shirt"]
                               ["In the Expa Office"]]}}
                (with-datomic
                  (data/dataset tupac-sightings
                    (data/run-mbql-query sightings
                      {:fields   [$category_id->categories.name]
                       :order-by [[:desc $timestamp]]
                       :limit    10})))))))

(deftest order-by-multiple-foreign-keys-test
  ;; 1. Check that we can order by Foreign Keys
  ;;    (this query targets sightings and orders by cities.name and categories.name)
  ;; 2. Check that we can join MULTIPLE tables in a single query
  ;;    (this query joins both cities and categories)
  (is (match? {:data {:columns ["name" "name"],
                      :rows [["Akron" "Working at a Pet Store"]
                             ["Akron" "Working as a Limo Driver"]
                             ["Akron" "Working as a Limo Driver"]
                             ["Akron" "Wearing a Biggie Shirt"]
                             ["Akron" "In the Mall"]
                             ["Akron" "At a Restaurant"]
                             ["Albany" "Working as a Limo Driver"]
                             ["Albany" "Working as a Limo Driver"]
                             ["Albany" "Wearing a Biggie Shirt"]
                             ["Albany" "Wearing a Biggie Shirt"]]}}
              (with-datomic
                (data/dataset tupac-sightings
                  (data/run-mbql-query sightings
                    {:fields [$city_id->cities.name
                              $category_id->categories.name]
                     :order-by [[:asc $city_id->cities.name]
                                [:desc $category_id->categories.name]
                                [:asc (data/id "sightings" "db/id")]]
                     :limit    10}))))))

(deftest multi-fk-single-table-test
  (testing "join against the same table twice via different paths"
    (is (match? {:data {:columns ["name" "count"]
                        :rows    [["Bob the Sea Gull" 2]
                                  ["Brenda Blackbird" 2]
                                  ["Lucky Pigeon"     2]
                                  ["Peter Pelican"    5]
                                  ["Ronald Raven"     1]]}}

                (with-datomic
                  (data/dataset avian-singles
                    (data/run-mbql-query messages
                      {:aggregation [[:count]]
                       :breakout    [$sender_id->users.name]
                       :filter      [:= $reciever_id->users.name "Rasta Toucan"]})))))))
