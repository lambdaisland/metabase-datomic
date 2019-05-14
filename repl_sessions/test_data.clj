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





[[5 86 #inst "2015-04-02T07:00:00.000-00:00"]
 [7 98 #inst "2015-04-04T07:00:00.000-00:00"]
 [1 97 #inst "2015-04-05T07:00:00.000-00:00"]
 [11 74 #inst "2015-04-06T07:00:00.000-00:00"]
 [5 12 #inst "2015-04-07T07:00:00.000-00:00"]
 [10 80 #inst "2015-04-08T07:00:00.000-00:00"]
 [11 73 #inst "2015-04-09T07:00:00.000-00:00"]
 [9 20 #inst "2015-04-09T07:00:00.000-00:00"]
 [8 51 #inst "2015-04-10T07:00:00.000-00:00"]
 [11 88 #inst "2015-04-10T07:00:00.000-00:00"]
 [10 44 #inst "2015-04-11T07:00:00.000-00:00"]
 [10 12 #inst "2015-04-12T07:00:00.000-00:00"]
 [5 38 #inst "2015-04-15T07:00:00.000-00:00"]
 [14 8 #inst "2015-04-16T07:00:00.000-00:00"]
 [12 5 #inst "2015-04-16T07:00:00.000-00:00"]
 [1 1 #inst "2015-04-18T07:00:00.000-00:00"]
 [9 21 #inst "2015-04-18T07:00:00.000-00:00"]
 [12 49 #inst "2015-04-19T07:00:00.000-00:00"]
 [8 84 #inst "2015-04-20T07:00:00.000-00:00"]
 [12 7 #inst "2015-04-21T07:00:00.000-00:00"]
 [7 98 #inst "2015-04-21T07:00:00.000-00:00"]
 [7 10 #inst "2015-04-23T07:00:00.000-00:00"]
 [12 2 #inst "2015-04-23T07:00:00.000-00:00"]
 [11 71 #inst "2015-04-24T07:00:00.000-00:00"]
 [6 22 #inst "2015-04-24T07:00:00.000-00:00"]
 [1 46 #inst "2015-04-25T07:00:00.000-00:00"]
 [11 65 #inst "2015-04-30T07:00:00.000-00:00"]
 [15 52 #inst "2015-05-01T07:00:00.000-00:00"]
 [13 86 #inst "2015-05-01T07:00:00.000-00:00"]]
