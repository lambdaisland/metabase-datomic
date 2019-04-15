(ns user.repl
  (:require clojure.tools.namespace.repl
            datomic.api
            metabase.driver.datomic
            metabase.query-processor
            [toucan.db :as db]
            [metabase.models.database :refer [Database]]
            [datomic.api :as d]
            [clojure.string :as str]))

(def mbrainz-url "datomic:free://localhost:4334/mbrainz")

(defn conn
  ([]
   (conn mbrainz-url))
  ([url]
   (datomic.api/connect url)))

(defn db
  ([]
   (db mbrainz-url))
  ([url]
   (datomic.api/db (conn url))))

(defn mbql-history []
  @metabase.driver.datomic/mbql-history)

(defn query-history []
  @metabase.driver.datomic/query-history)

(defn qry
  ([]
   (first (mbql-history)))
  ([n]
   (nth (mbql-history) n)))

(defn nqry
  ([]
   (first (query-history)))
  ([n]
   (nth (query-history) n)))

(defn query->native [q]
  (metabase.query-processor/query->native q))

(defn clean-up-in-mem-dbs []
  (doseq [{{uri :db} :details id :id}
          (db/select Database :engine "datomic")
          :when (str/includes? uri ":mem:")]
    (try
      (d/connect uri)
      (catch Exception e
        (db/delete! Database :id id)))))

;; (d/delete-database "datomic:mem:countries")
;; (clean-up-in-mem-dbs)

(defn bind-driver! []
  (alter-var-root #'metabase.driver/*driver* (constantly :datomic)))
