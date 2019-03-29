(ns user.repl
  (:require clojure.tools.namespace.repl
            datomic.api
            metabase.driver.datomic
            metabase.query-processor))

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

(defn query->native [q]
  (metabase.query-processor/query->native q))
