(ns test-data
  (:require [datomic.api :as d]
            [metabase.driver.datomic.test :refer [with-datomic]]
            [metabase.test.data :as data]))

(with-datomic
  (data/get-or-create-test-data-db!))

(def url "datomic:mem:test-data")
(def conn (d/connect url))
(defn db [] (d/db conn))

(d/q
 '{:find     [?checkins|checkins|user_id]
   :order-by [[:asc (:checkins/user_id ?checkins)]],
   :where    [[?checkins :checkins/user_id ?checkins|checkins|user_id]],
   :select   [(:checkins/user_id ?checkins)],
   }
 (db))
