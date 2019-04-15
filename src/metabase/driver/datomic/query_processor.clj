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
  {:pre [(instance? datomic.db.Db db)
         (string? table)]}
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
               db))
        sort)))

(def connect #_(memoize d/connect)
  d/connect)

(defn db []
  (-> (get-in (qp.store/database) [:details :db]) connect d/db))

(defn into-clause
  ([qry clause coll]
   (into-clause qry clause identity coll))
  ([qry clause xform coll]
   (if (seq coll)
     (update qry clause (fn [x] (into (or x []) xform coll)))
     qry)))

(defmulti ->attrib mbql.u/dispatch-by-clause-name-or-class)

(defmethod ->attrib (class Field) [{:keys [name table_id] :as field}]
  (if (some #{\/} name)
    (keyword name)
    (keyword (:name (qp.store/table table_id)) name)))

(defmethod ->attrib :field-id [[_ field-name]]
  (->attrib (qp.store/field field-name)))

(defmethod ->attrib :aggregation [[_ field-name]]
  (->attrib (qp.store/field field-name)))

(def field-ref nil)

(defmulti field-ref (fn [field-ref qry]
                      (mbql.u/dispatch-by-clause-name-or-class field-ref)))

(defmethod field-ref :field-id [f qry]
  [:entity (count (:find qry)) (->attrib f)])

;; (defmethod field-ref :aggregation [_ _]
;;   ;; MBQL only supports a single aggregation, we only add it first in the result
;;   [:nth 0])

(defn apply-source-table [qry {:keys [source-table]} db]
  (let [table   (qp.store/table source-table)
        columns (map first (table-columns db (:name table)))
        clause  `(~'or ~@(map #(vector '?eid %) columns))]
    (into-clause qry :where [clause])))

(defn apply-fields [qry {:keys [fields]} db]
  (if (seq fields)
    (-> qry
        (into-clause :find ['?eid])
        (into-clause :fields
                     (map #(field-ref % qry))
                     fields))
    qry))

;; breakouts with aggregation = GROUP BY
;; breakouts without aggregation = SELECT DISTINCT
(defn apply-breakouts [qry {:keys [breakout]} db]
  (if (seq breakout)
    (-> qry
        (into-clause :find
                     (map (fn [idx]
                            (symbol (str "?breakout-" idx))))
                     (range (count breakout)))
        (into-clause :where
                     (map-indexed (fn [idx f]
                                    ['?eid
                                     (->attrib f)
                                     (symbol (str "?breakout-" idx))]))
                     breakout)
        (into-clause :fields
                     (map (fn [idx] [:nth idx]))
                     (range (count (:find qry))
                            (+ (count (:find qry)) (count breakout)))))
    qry))

(defn apply-aggregation [qry {:keys [aggregation]} db]
  (if (= :count (ffirst aggregation))
    (-> qry
        (into-clause :find ['(count ?eid)])
        (into-clause :fields [[:nth (count (:find qry))]]))
    qry))

(defn apply-order-by [qry {:keys [order-by]}]
  (if (seq order-by)
    (into-clause qry :order-by (map (juxt first (comp #(field-ref % {}) second))) order-by)
    qry))

(defn mbql->native [{:keys [database query] :as mbql-query}]
  (let [db (db)]
    {:db db
     :query
     (with-out-str
       (clojure.pprint/pprint
        (-> {}
            (apply-source-table query db)
            (apply-fields query db)
            (apply-order-by query)
            (apply-breakouts query db)
            (apply-aggregation query db)
            )))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti field-lookup (fn [[f _] _] f))

(defmethod field-lookup :nth [[_ idx] _]
  #(nth % idx))

(defmethod field-lookup :entity [[_ idx attr] entity]
  #(do
     (prn [:FFF % idx attr])
     (get (entity (nth % idx)) attr)))

(defn juxt-fields [entity field-lookups]
  (apply juxt (map #(field-lookup % entity) field-lookups)))

(defn order-by->comparator [entity order-by]
  (fn [x y]
    (prn [x y])
    (reduce (fn [result [dir field-ref]]
              (prn field-ref)
              (if (= 0 result)
                (*
                 (if (= :desc dir) -1 1)
                 (compare ((field-lookup field-ref entity) x)
                          ((field-lookup field-ref entity) y)))
                (reduced result)))
            0
            order-by)))

(defn order-by-attribs [entity order-by results]
  (if (seq order-by)
    (sort (order-by->comparator entity order-by) results)
    results))

(defn resolve-fields [db result {:keys [fields order-by] :as datalog}]
  (let [entity (memoize (fn [eid] (d/entity db eid)))]
    (->> (map (juxt-fields entity fields) result)
         (order-by-attribs entity order-by))))

(defmulti col-name mbql.u/dispatch-by-clause-name-or-class)

(defmethod col-name :field-id [[_ id]]
  (:name (qp.store/field id)))

(defn result-map-mbql
  "Result map for a query originating from Metabase directly. We have access to
  the original MBQL query."
  [db results datalog query]
  (let [{:keys [source-table fields limit breakout aggregation]} query]
    {:columns (concat (map col-name fields)
                      (map col-name breakout)
                      (if aggregation
                        [(name (ffirst aggregation))]))
     :rows    (resolve-fields db results datalog)}))

(defn result-map-native
  "Result map for a 'native' query entered directly by the user."
  [db results datalog]
  {:columns (map str (:find datalog))
   :rows (seq results)})

(def reslt (atom nil))
@reslt



(defn execute-query [{:keys [native query] :as native-query}]
  (prn query)
  (prn native)
  (prn (:settings native-query))
  (let [db      (or (:db native) (db))
        datalog (read-string (:query native))
        results (d/q (dissoc datalog :fields) db)
        x       (if query
                  (result-map-mbql db results datalog query)
                  (result-map-native db results datalog))]
    (reset! reslt x)
    x))
