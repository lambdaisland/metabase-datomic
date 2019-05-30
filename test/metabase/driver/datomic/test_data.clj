(ns metabase.driver.datomic.test-data
  (:require [metabase.test.data.interface :as tx]))

(defn rows+cols [result]
  (select-keys (:data result) [:columns :rows]))

(tx/defdataset aggr-data
  [["foo"
    [{:field-name "f1" :base-type :type/Text}
     {:field-name "f2" :base-type :type/Text}]
    [["xxx" "a"]
     ["xxx" "b"]
     ["yyy" "c"]]]])

(tx/defdataset countries
  [["country"
    [{:field-name "code" :base-type :type/Text}
     {:field-name "name" :base-type :type/Text}]
    [["BE" "Belgium"]
     ["DE" "Germany"]
     ["FI" "Finnland"]]]])

(tx/defdataset with-nulls
  [["country"
    [{:field-name "code" :base-type :type/Text}
     {:field-name "name" :base-type :type/Text}
     {:field-name "population" :base-type :type/Integer}
     ]
    [["BE" "Belgium" 11000000]
     ["DE" "Germany"]
     ["FI" "Finnland"]]]])

#_(user.setup/remove-database "countries")
