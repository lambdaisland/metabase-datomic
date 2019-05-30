(ns metabase.driver.datomic.fields-test
  (:require [clojure.test :refer :all]
            [metabase.driver.datomic.test :refer :all]
            [metabase.driver.datomic.test-data :as test-data]
            [metabase.test.data :as data]
            [toucan.db :as db]
            [matcher-combinators.matchers :as m]))

(deftest fields-test
  (let [result
        (with-datomic
          (let [id [:field-id (data/id :venues "db/id")]]
            (data/run-mbql-query venues
              {:fields   [$name id]
               :limit    10
               :order-by [[:asc id]]})))]

    (testing "we get the right fields back"
      (is (match?
           {:row_count 10
            :status    :completed
            :data
            {:rows        [["Red Medicine"                  pos-int?]
                           ["Stout Burgers & Beers"         pos-int?]
                           ["The Apple Pan"                 pos-int?]
                           ["Wurstküche"                    pos-int?]
                           ["Brite Spot Family Restaurant"  pos-int?]
                           ["The 101 Coffee Shop"           pos-int?]
                           ["Don Day Korean Restaurant"     pos-int?]
                           ["25°"                           pos-int?]
                           ["Krua Siri"                     pos-int?]
                           ["Fred 62"                       pos-int?]]
             :columns     ["name" "db/id"]
             :cols        [{:base_type       :type/Text
                            :special_type    :type/Name
                            :name            "name"
                            :display_name    "Name"
                            :source          :fields
                            :visibility_type :normal}
                           {:base_type       :type/PK
                            :special_type    :type/PK
                            :name            "db/id"
                            :display_name    "Db/id"
                            :source          :fields
                            :visibility_type :normal}]
             :native_form {:query string?}}}
           result)))

    (testing "returns all ids in order"
      (let [ids (map last (get-in result [:data :rows]))]
        (is (= (sort ids) ids))))))

(deftest basic-query-test
  (with-datomic
    (is (match? {:columns ["db/id" "name" "code"]
                 :rows    (m/in-any-order
                           [[pos-int? "Belgium" "BE"]
                            [pos-int? "Germany" "DE"]
                            [pos-int? "Finnland" "FI"]])}
                (test-data/rows+cols
                 (data/with-db-for-dataset [_ test-data/countries]
                   (data/run-mbql-query country)))))

    (is (match?
         {:data {:columns ["name" "db/id"],
                 :rows [["20th Century Cafe" pos-int?]
                        ["25°" pos-int?]
                        ["33 Taps" pos-int?]
                        ["800 Degrees Neapolitan Pizzeria" pos-int?]
                        ["BCD Tofu House" pos-int?]
                        ["Baby Blues BBQ" pos-int?]
                        ["Barney's Beanery" pos-int?]
                        ["Beachwood BBQ & Brewing" pos-int?]
                        ["Beyond Sushi" pos-int?]
                        ["Bludso's BBQ" pos-int?]]}}
         (data/run-mbql-query venues
           {:fields   [$name [:field-id (data/id :venues "db/id")]]
            :limit    10
            :order-by [[:asc $name]]})))))
