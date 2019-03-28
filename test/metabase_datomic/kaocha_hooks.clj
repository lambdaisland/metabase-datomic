(ns metabase-datomic.kaocha-hooks
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [metabase.models.database :as database :refer [Database]]
            [metabase.util :as util]
            [toucan.db :as db]
            [metabase.test-setup :as test-setup]
            [clojure.string :as str]
            [datomic.api :as d]))

(defn pre-run [test-plan]
  (test-setup/test-startup)
  test-plan)

(defn post-run [test-plan]
  (doseq [{:keys [id name details]} (db/select Database {:engine :datomic})
          :when (str/includes? (:db details) ":mem:")]
    (util/ignore-exceptions
      (d/delete-database (:db details)))
    (db/delete! Database {:id id}))
  (test-setup/test-teardown)
  test-plan)
