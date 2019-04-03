(ns metabase.driver.datomic.query-processor
  (:require [metabase.driver :as driver]
            [metabase.query-processor :as qp]
            [metabase.query-processor.store :as qp.store]
            [metabase.models.field :as field :refer [Field]]
            [datomic.api :as d]
            [clojure.string :as str]
            [metabase.mbql.util :as mbql.u]
            [metabase.driver.datomic.util :as util]))

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

(def connect (memoize d/connect))

(defn db []
  (-> (get-in (qp.store/database) [:details :db]) connect d/db))

(defn into-clause
  ([qry clause coll]
   (into-clause qry clause identity coll))
  ([qry clause xform coll]
   (if (seq coll)
     (update qry clause (fn [x] (into (or x []) xform coll)))
     qry)))

(defmulti ->rvalue mbql.u/dispatch-by-clause-name-or-class)

(defmethod ->rvalue (class Field) [{:keys [name table_id] :as field}]
  (if (some #{\/} name)
    (keyword name)
    (keyword (:name (qp.store/table table_id)) name)))

(defmethod ->rvalue :field-id [[_ field-name]]
  (->rvalue (qp.store/field field-name)))

(defmethod ->rvalue :aggregation [[_ field-name]]
  (->rvalue (qp.store/field field-name)))

(def field-lookup nil)

(defmulti field-lookup (fn [field-ref qry]
                         (mbql.u/dispatch-by-clause-name-or-class field-ref)))

(defmethod field-lookup :field-id [f qry]
  [:entity (count (:find qry)) (->rvalue f)])

;; (defmethod field-lookup :aggregation [_ _]
;;   ;; MBQL only supports a single aggregation, we only add it first in the result
;;   [:nth 0])

(defn apply-source-table [result {:keys [source-table]} db]
  (let [table   (qp.store/table source-table)
        columns (map first (table-columns db (:name table)))
        clause  `(~'or ~@(map #(vector '?eid %) columns))]
    (into-clause result :where [clause])))

(defn apply-fields [result {:keys [fields]} db]
  (if (seq fields)
    (-> result
        (into-clause :find ['?eid])
        (into-clause :fields
                     (map #(field-lookup % result))
                     fields))
    result))

;; breakouts with aggregation = GROUP BY
;; breakouts without aggregation = SELECT DISTINCT
(defn apply-breakouts [result {:keys [breakout]} db]
  (if (seq breakout)
    (-> result
        (into-clause :find
                     (map (fn [idx]
                            (symbol (str "?breakout-" idx))))
                     (range (count breakout)))
        (into-clause :where
                     (map-indexed (fn [idx f]
                                    ['?eid
                                     (->rvalue f)
                                     (symbol (str "?breakout-" idx))]))
                     breakout)
        (into-clause :fields
                     (map (fn [idx] [:nth idx]))
                     (range (count (:find result))
                            (+ (count (:find result)) (count breakout)))))
    result))

(defn apply-aggregation [result {:keys [aggregation]} db]
  (if (= :count (ffirst aggregation))
    (-> result
        (into-clause :find ['(count ?eid)])
        (into-clause :fields [[:nth (count (:find result))]]))
    result))

(defn mbql->native [{:keys [database query] :as mbql-query}]
  (let [db (db)]
    {:db db
     :query
     (with-out-str
       (clojure.pprint/pprint
        (-> {}
            (apply-source-table query db)
            (apply-fields query db)
            (apply-breakouts query db)
            (apply-aggregation query db))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti field-lookup (fn [[f _] _] f))

(defmethod field-lookup :nth [[_ idx] _]
  #(nth % idx))

(defmethod field-lookup :entity [[_ idx attr] entity]
  #(get (entity (nth % idx)) attr))

(defn resolve-fields [db result field-lookups]
  (let [entity (memoize (fn [eid] (d/entity db eid)))
        lookup (apply juxt (map #(field-lookup % entity) field-lookups))]
    (map lookup result)))

(def reslt (atom nil))

(defn execute-query [{:keys [native query] :as native-query}]
  (let [{:keys [source-table fields limit breakout aggregation]} query

        db      (:db native)
        datalog (read-string (:query native))
        results (d/q (dissoc datalog :fields) db)
        x {:columns (concat (map (comp util/kw->str ->rvalue) fields)
                            (map (comp util/kw->str ->rvalue) breakout)
                            (if aggregation
                              [(name (ffirst aggregation))]))
           :rows    (resolve-fields db results (:fields datalog))}]
    (reset! reslt x)
    x
    ))

@reslt
