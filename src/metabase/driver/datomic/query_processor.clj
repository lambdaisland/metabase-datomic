(ns metabase.driver.datomic.query-processor
  (:require [metabase.driver :as driver]
            [metabase.query-processor :as qp]
            [metabase.query-processor.store :as qp.store]
            [datomic.api :as d]
            [clojure.string :as str]))

(defn attributes
  "Query db for all attribute entities."
  [db]
  (->> db
       (d/q '{:find [[?eid ...]] :where [[?eid :db/valueType]]})
       (map (partial d/entity db))))

(defn attrs-by-table [db]
  (reduce #(update %1 (namespace (:db/ident %2)) conj %2)
          {}
          (attributes db)))

(defn derive-table-names
  "Find all \"tables\" i.e. all namespace prefixes used in attribute names."
  [db]
  (remove #{"fressian" "db" "db.alter" "db.excise" "db.install" "db.sys"}
          (keys (attrs-by-table db))))

(defn table-columns
  "Given the name of a \"table\" (attribute namespace prefix), find all attribute
  names that occur in entities that have an attribute with this prefix."
  [db table]
  (let [attrs (get (attrs-by-table db) table)]
    (-> #{}
        (into (map (juxt :db/ident :db/valueType))
              attrs)
        (into (d/q
               {:find '[?ident ?type]
                :where [(cons 'or
                              (for [attr attrs]
                                ['?eid (:db/ident attr)]))
                        '[?eid ?attr]
                        '[?attr :db/ident ?ident]
                        '[?attr :db/valueType ?type-id]
                        '[?type-id :db/ident ?type]
                        '[(not= ?ident :db/ident)]]}
               db)))))

(defn mbql->native [{:keys [database query] :as mbql-query}]
  (let [{:keys [source-table fields limit]} query

        uri     (get-in (qp.store/database) [:details :db])
        conn    (d/connect uri)
        db      (d/db conn)
        table   (qp.store/table source-table)
        columns (map first (table-columns db (:name table)))
        fields  (mapv (comp keyword
                            :name
                            (fn [[_ f]] (qp.store/field f)))
                      fields)]
    {:db db
     :query
     (with-out-str
       (clojure.pprint/pprint
        {:find  [(list 'pull '?e fields)]
         :where [`(~'or ~@(map #(vector '?e %) columns))]}))}))
