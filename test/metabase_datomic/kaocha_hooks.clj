(ns metabase-datomic.kaocha-hooks
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [metabase.models.database :as database :refer [Database]]
            [metabase.util :as util]
            [toucan.db :as db]
            [metabase.test-setup :as test-setup]
            [clojure.string :as str]
            [datomic.api :as d]))

(defonce startup-once
  (delay
   (test-setup/test-startup)
   (require 'user)
   (require 'user.repl)
   ((resolve 'user.repl/clean-up-in-mem-dbs))
   ((resolve 'user/setup-driver!))))

(defn pre-load [test-plan]
  @startup-once
  test-plan)

(defn post-run [test-plan]
  #_(doseq [{:keys [id name details]} (db/select Database {:engine :datomic})
            :when (str/includes? (:db details) ":mem:")]
      (util/ignore-exceptions
        (d/delete-database (:db details)))
      (db/delete! Database {:id id}))
  #_(test-setup/test-teardown)
  test-plan)
