(ns metabase.driver.datomic.breakout-test
  (:require [cheshire.core :as json]
            [clojure.test :refer :all]
            [datomic.api :as d]
            [metabase.driver.datomic.query-processor :as datomic.qp]
            [metabase.driver.datomic.test :refer :all]
            [metabase.driver.datomic.util :refer [pal]]
            metabase.models.database
            [metabase.models.dimension :refer [Dimension]]
            [metabase.models.field :refer [Field]]
            [metabase.models.field-values :refer [FieldValues]]
            [metabase.test.data :as data]
            [metabase.test.data.dataset-definitions :as defs]
            [toucan.db :as db]))

(deftest breakout-single-column-test
  (let [result (with-datomic
                 (data/run-mbql-query checkins
                   {:aggregation [[:count]]
                    :breakout    [$user_id]
                    :order-by    [[:asc $user_id]]}))]

    (is (match? {:data {:rows    [[pos-int? 31] [pos-int? 70] [pos-int? 75]
                                  [pos-int? 77] [pos-int? 69] [pos-int? 70]
                                  [pos-int? 76] [pos-int? 81] [pos-int? 68]
                                  [pos-int? 78] [pos-int? 74] [pos-int? 59]
                                  [pos-int? 76] [pos-int? 62] [pos-int? 34]]
                        :columns ["user_id"
                                  "count"]}}
                result))

    (is (= (get-in result [:data :rows])
           (sort-by first (get-in result [:data :rows]))))))

(deftest breakout-aggregation-test
  (testing "This should act as a \"distinct values\" query and return ordered results"
    (is (match? {:data
                 {:columns ["price"],
                  :rows [[1] [2] [3] [4]]
                  :cols [{:name "price"}]}}

                (with-datomic
                  (data/run-mbql-query venues
                    {:breakout [$price]
                     :limit    10}))))))

(deftest breakout-multiple-columns-implicit-order
  (testing "Fields should be implicitly ordered :ASC for all the fields in `breakout` that are not specified in `order-by`"
    (is (match? {:data
                 {:columns ["user_id" "venue_id" "count"]
                  :rows
                  (fn [rows]
                    (= rows (sort-by (comp vec (pal take 2)) rows)))}}
                (with-datomic
                  (data/run-mbql-query checkins
                    {:aggregation [[:count]]
                     :breakout    [$user_id $venue_id]
                     :limit       10}))))))

(deftest breakout-multiple-columns-explicit-order
  (testing "`breakout` should not implicitly order by any fields specified in `order-by`"
    (is (match?
         {:data
          {:columns ["name" "price" "count"]
           :rows [["bigmista's barbecue" 2 1]
                  ["Zeke's Smokehouse" 2 1]
                  ["Yuca's Taqueria" 1 1]
                  ["Ye Rustic Inn" 1 1]
                  ["Yamashiro Hollywood" 3 1]
                  ["Wurstküche" 2 1]
                  ["Two Sisters Bar & Books" 2 1]
                  ["Tu Lan Restaurant" 1 1]
                  ["Tout Sweet Patisserie" 2 1]
                  ["Tito's Tacos" 1 1]]}}
         (with-datomic
           (data/run-mbql-query venues
             {:aggregation [[:count]]
              :breakout    [$name $price]
              :order-by    [[:desc $name]]
              :limit       10}))))))

(defn test-data-categories []
  (sort
   (d/q '{:find [?cat ?id]
          :where [[?id :categories/name ?cat]]}
        (d/db (d/connect "datomic:mem:test-data")))))

(deftest remapped-column
  (testing "breakout returns the remapped values for a custom dimension"
    (is (match?
         {:data
          {:rows    [[pos-int? 8 "Blues"]
                     [pos-int? 2 "Rock"]
                     [pos-int? 2 "Swing"]
                     [pos-int? 7 "African"]
                     [pos-int? 2 "American"]]
           :columns ["category_id" "count" "Foo"]
           :cols    [{:name "category_id" :remapped_to "Foo"}
                     {:name "count"}
                     {:name "Foo" :remapped_from "category_id"}]}}
         (with-datomic
           (data/with-data
             #(let [categories (test-data-categories)
                    cat-names  (->> categories
                                    (map first)
                                    (concat ["Jazz" "Blues" "Rock" "Swing"])
                                    (take (count categories)))
                    cat-ids    (map last categories)]
                [(db/insert! Dimension
                   {:field_id (data/id :venues :category_id)
                    :name     "Foo"
                    :type     :internal})
                 (db/insert! FieldValues
                   {:field_id              (data/id :venues :category_id)
                    :values                (json/generate-string cat-ids)
                    :human_readable_values (json/generate-string cat-names)})])
             (data/run-mbql-query venues
               {:aggregation [[:count]]
                :breakout    [$category_id]
                :limit       5})))))))

(deftest order-by-custom-dimension
  (is (= [["Wine Bar" "Thai" "Thai" "Thai" "Thai" "Steakhouse" "Steakhouse"
           "Steakhouse" "Steakhouse" "Southern"]
          ["American" "American" "American" "American" "American" "American"
           "American" "American" "Artisan" "Artisan"]]
         (with-datomic
           (data/with-data
             (fn []
               [(db/insert! Dimension
                  {:field_id                (data/id :venues :category_id)
                   :name                    "Foo"
                   :type                    :external
                   :human_readable_field_id (data/id :categories :name)})])
             [(->> (data/run-mbql-query venues
                     {:order-by [[:desc $category_id]]
                      :limit    10})
                   :data :rows
                   (map last))
              (->> (data/run-mbql-query venues
                     {:order-by [[:asc $category_id]]
                      :limit    10})
                   :data :rows
                   (map last))])))))


(comment
  ;; We can convert more of these once we implement binning

  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :binning)
                                [[10.0 1] [32.0 4] [34.0 57] [36.0 29] [40.0 9]]
                                (format-rows-by [(partial u/round-to-decimals 1) int]
                                                (rows (data/run-mbql-query venues
                                                        {:aggregation [[:count]]
                                                         :breakout    [[:binning-strategy $latitude :num-bins 20]]}))))

  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :binning)
                                [[0.0 1] [20.0 90] [40.0 9]]
                                (format-rows-by [(partial u/round-to-decimals 1) int]
                                                (rows (data/run-mbql-query venues
                                                        {:aggregation [[:count]]
                                                         :breakout    [[:binning-strategy $latitude :num-bins 3]]}))))

  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :binning)
                                [[10.0 -170.0 1] [32.0 -120.0 4] [34.0 -120.0 57] [36.0 -125.0 29] [40.0 -75.0 9]]
                                (format-rows-by [(partial u/round-to-decimals 1) (partial u/round-to-decimals 1) int]
                                                (rows (data/run-mbql-query venues
                                                        {:aggregation [[:count]]
                                                         :breakout    [[:binning-strategy $latitude :num-bins 20]
                                                                       [:binning-strategy $longitude :num-bins 20]]}))))

  ;; Currently defaults to 8 bins when the number of bins isn't
  ;; specified
  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :binning)
                                [[10.0 1] [30.0 90] [40.0 9]]
                                (format-rows-by [(partial u/round-to-decimals 1) int]
                                                (rows (data/run-mbql-query venues
                                                        {:aggregation [[:count]]
                                                         :breakout    [[:binning-strategy $latitude :default]]}))))

  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :binning)
                                [[10.0 1] [30.0 61] [35.0 29] [40.0 9]]
                                (tu/with-temporary-setting-values [breakout-bin-width 5.0]
                                  (format-rows-by [(partial u/round-to-decimals 1) int]
                                                  (rows (data/run-mbql-query venues
                                                          {:aggregation [[:count]]
                                                           :breakout    [[:binning-strategy $latitude :default]]})))))

  ;; Testing bin-width
  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :binning)
                                [[10.0 1] [33.0 4] [34.0 57] [37.0 29] [40.0 9]]
                                (format-rows-by [(partial u/round-to-decimals 1) int]
                                                (rows (data/run-mbql-query venues
                                                        {:aggregation [[:count]]
                                                         :breakout    [[:binning-strategy $latitude :bin-width 1]]}))))

  ;; Testing bin-width using a float
  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :binning)
                                [[10.0 1] [32.5 61] [37.5 29] [40.0 9]]
                                (format-rows-by [(partial u/round-to-decimals 1) int]
                                                (rows (data/run-mbql-query venues
                                                        {:aggregation [[:count]]
                                                         :breakout    [[:binning-strategy $latitude :bin-width 2.5]]}))))

  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :binning)
                                [[33.0 4] [34.0 57]]
                                (tu/with-temporary-setting-values [breakout-bin-width 1.0]
                                  (format-rows-by [(partial u/round-to-decimals 1) int]
                                                  (rows (data/run-mbql-query venues
                                                          {:aggregation [[:count]]
                                                           :filter      [:and
                                                                         [:< $latitude 35]
                                                                         [:> $latitude 20]]
                                                           :breakout    [[:binning-strategy $latitude :default]]})))))

  (defn- round-binning-decimals [result]
    (let [round-to-decimal #(u/round-to-decimals 4 %)]
      (-> result
          (update :min_value round-to-decimal)
          (update :max_value round-to-decimal)
          (update-in [:binning_info :min_value] round-to-decimal)
          (update-in [:binning_info :max_value] round-to-decimal))))

  ;;Validate binning info is returned with the binning-strategy
  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :binning)
                                (assoc (breakout-col (venues-col :latitude))
                                  :binning_info {:min_value 10.0, :max_value 50.0, :num_bins 4, :bin_width 10.0, :binning_strategy :bin-width})
                                (-> (data/run-mbql-query venues
                                      {:aggregation [[:count]]
                                       :breakout    [[:binning-strategy $latitude :default]]})
                                    tu/round-fingerprint-cols
                                    (get-in [:data :cols])
                                    first))

  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :binning)
                                (assoc (breakout-col (venues-col :latitude))
                                  :binning_info {:min_value 7.5, :max_value 45.0, :num_bins 5, :bin_width 7.5, :binning_strategy :num-bins})
                                (-> (data/run-mbql-query venues
                                      {:aggregation [[:count]]
                                       :breakout    [[:binning-strategy $latitude :num-bins 5]]})
                                    tu/round-fingerprint-cols
                                    (get-in [:data :cols])
                                    first))

  ;;Validate binning info is returned with the binning-strategy
  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :binning)
                                {:status :failed
                                 :class  Exception
                                 :error  "Unable to bin Field without a min/max value"}
                                (tu/with-temp-vals-in-db Field (data/id :venues :latitude) {:fingerprint {:type {:type/Number {:min nil, :max nil}}}}
                                  (-> (tu.log/suppress-output
                                       (data/run-mbql-query venues
                                         {:aggregation [[:count]]
                                          :breakout    [[:binning-strategy $latitude :default]]}))
                                      (select-keys [:status :class :error]))))

  (defn- field->result-metadata [field]
    (select-keys field [:name :display_name :description :base_type :special_type :unit :fingerprint]))

  (defn- nested-venues-query [card-or-card-id]
    {:database metabase.models.database/virtual-id
     :type     :query
     :query    {:source-table (str "card__" (u/get-id card-or-card-id))
                :aggregation  [:count]
                :breakout     [[:binning-strategy [:field-literal (data/format-name :latitude) :type/Float] :num-bins 20]]}})

  ;; Binning should be allowed on nested queries that have result metadata
  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :binning :nested-queries)
                                [[10.0 1] [32.0 4] [34.0 57] [36.0 29] [40.0 9]]
                                (tt/with-temp Card [card {:dataset_query   {:database (data/id)
                                                                            :type     :query
                                                                            :query    {:source-query {:source-table (data/id :venues)}}}
                                                          :result_metadata (mapv field->result-metadata (db/select Field :table_id (data/id :venues)))}]
                                  (->> (nested-venues-query card)
                                       qp/process-query
                                       rows
                                       (format-rows-by [(partial u/round-to-decimals 1) int]))))

  ;; Binning is not supported when there is no fingerprint to determine boundaries
  (datasets/expect-with-drivers (non-timeseries-drivers-with-feature :binning :nested-queries)
                                Exception
                                (tu.log/suppress-output
                                 (tt/with-temp Card [card {:dataset_query {:database (data/id)
                                                                           :type     :query
                                                                           :query    {:source-query {:source-table (data/id :venues)}}}}]
                                   (-> (nested-venues-query card)
                                       qp/process-query
                                       rows))))

  ;; if we include a Field in both breakout and fields, does the query still work? (Normalization should be taking care
  ;; of this) (#8760)
  (expect-with-non-timeseries-dbs
   :completed
   (-> (qp/process-query
        {:database (data/id)
         :type     :query
         :query    {:source-table (data/id :venues)
                    :breakout     [[:field-id (data/id :venues :price)]]
                    :fields       [["field_id" (data/id :venues :price)]]}})
       :status))
  )
