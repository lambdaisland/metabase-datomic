(ns test-data
  (:require [datomic.api :as d]
            [metabase.driver.datomic.test :refer [with-datomic]]
            [metabase.test.data :as data]))

(user/refer-repl)

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


(d/q
 '{:find [?medium]
   :where
   [(or [?medium :medium/format] [?medium :medium/name] [?medium :medium/position] [?medium :medium/trackCount] [?medium :medium/tracks])
    [?medium :medium/format ?medium|medium|format]
    [?medium|medium|format :db/ident ?i]
    [(= ?i :medium.format/vinyl)]],
   ;;:select [(:db/id ?medium) (:medium/name ?medium) (:medium/format ?medium) (:medium/position ?medium) (:medium/trackCount ?medium) (:medium/tracks ?medium)],
   :with ()}
 (db))

(d/q
 '{;;:order-by [[:asc (:medium/name ?medium)] [:asc (:medium/format ?medium)]],
   :find [?medium|medium|name #_ ?medium|medium|format],
   :where [[?medium :medium/name ?medium|medium|name]
           #_[?medium :medium/format ?medium|medium|format]],
   ;; :select [(:medium/name ?medium) (:medium/format ?medium)],
   :with ()}
 (db))
