(ns metabase.driver.datomic.query-processor
  (:require [metabase.driver :as driver]
            [metabase.query-processor :as qp]
            [metabase.query-processor.store :as qp.store]
            [metabase.models.field :as field :refer [Field]]
            [datomic.api :as d]
            [clojure.string :as str]
            [metabase.mbql.util :as mbql.u]
            [metabase.driver.datomic.util :as util :refer [pal par]]
            [toucan.db :as db]
            [metabase.models.table :refer [Table]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SCHEMA

(def reserved-prefixes
  #{"fressian"
    "db"
    "db.alter"
    "db.excise"
    "db.install"
    "db.sys"})

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
  (remove reserved-prefixes
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; QUERY->NATIVE

(defn into-clause
  ([qry clause coll]
   (into-clause qry clause identity coll))
  ([qry clause xform coll]
   (if (seq coll)
     (update qry clause (fn [x] (into (or x []) xform coll)))
     qry)))

;; [:field-id 55] => :artist/name
(defmulti ->attrib mbql.u/dispatch-by-clause-name-or-class)

(defmethod ->attrib (class Field) [{:keys [name table_id] :as field}]
  (if (some #{\/} name)
    (keyword name)
    (keyword (:name (qp.store/table table_id)) name)))

(defmethod ->attrib :field-id [[_ field-name]]
  (->attrib (qp.store/field field-name)))

(defmethod ->attrib :aggregation [[_ field-name]]
  (->attrib (qp.store/field field-name)))

;; [:field-id 55] => ?artist
(defmulti ->eid mbql.u/dispatch-by-clause-name-or-class)

(defmethod ->eid (class Table) [{:keys [name]}]
  (symbol (str "?" name)))

(defmethod ->eid (class Field) [{:keys [table_id]}]
  (->eid (qp.store/table table_id)))

(defmethod ->eid Integer [table_id]
  (->eid (qp.store/table table_id)))

(defmethod ->eid :field-id [[_ field-id]]
  (->eid (qp.store/field field-id)))

(defmulti field-ref mbql.u/dispatch-by-clause-name-or-class)

(defmethod field-ref :field-id [field-ref]
  (list (->attrib field-ref)
        (->eid field-ref)))

;; (defmethod field-ref :aggregation [_ _]
;;   ;; MBQL only supports a single aggregation, we only add it first in the result
;;   [:nth 0])

(defn apply-source-table [qry {:keys [source-table]} db]
  (let [table   (qp.store/table source-table)
        eid     (->eid table)
        fields  (db/select Field :table_id source-table)
        attribs (remove (comp reserved-prefixes namespace)
                        (map ->attrib fields))
        clause  `(~'or ~@(map #(vector eid %) attribs))]
    (-> qry
        (into-clause :where [clause]))))

;; Entries in the :fields clause can be
;;
;; | Concrete field refrences | [:field-id 15]           |
;; | Expression references    | [:expression :sales_tax] |
;; | Aggregates               | [:aggregate 0]           |
;; | Foreign keys             | [:fk-> 10 20]            |
(defn apply-fields [qry {:keys [source-table fields order-by]} db]
  (if (seq fields)
    (-> qry
        (into-clause :find [(->eid source-table)])
        (into-clause :select (map field-ref) fields))
    qry))

;; breakouts with aggregation = GROUP BY
;; breakouts without aggregation = SELECT DISTINCT
(defn apply-breakouts [qry {:keys [breakout order-by]} db]
  (if (seq breakout)
    (let [breakout-sym (fn [field-ref]
                         (let [attr (->attrib field-ref)
                               eid (->eid field-ref)]
                           (symbol (str eid
                                        "|"
                                        (namespace attr)
                                        "|"
                                        (name attr)))))]
      (-> qry
          (into-clause :find (map breakout-sym) breakout)
          (into-clause :with (map ->eid) breakout)
          (into-clause :where (map (juxt ->eid ->attrib breakout-sym)) breakout)
          (into-clause :select (map field-ref) breakout)))
    qry))

(defmulti apply-aggregation (fn [qry {:keys [aggregation]} db]
                              (ffirst aggregation)))

(defmethod apply-aggregation :default [qry _ _] qry)

(defmethod apply-aggregation :count [qry {:keys [aggregation]} db]
  ;; TODO: this probably isn't accurate, but Datomic doesn't have a COUNT(*) or
  ;; COUNT(1)
  (let [count-clause (list 'count (first (:find qry)))]
    (-> qry
        (into-clause :find [count-clause])
        (into-clause :select [count-clause]))))

(defn apply-order-by [qry {:keys [order-by]}]
  (if (seq order-by)
    (into-clause qry :order-by (map (juxt first (comp field-ref second))) order-by)
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
;; EXECUTE-QUERY

(defn index-of [xs x]
  (loop [idx 0
         [y & xs] xs]
    (if (= x y)
      idx
      (if (seq xs)
        (recur (inc idx) xs)))))

(defn select-field
  "Returns a function which, given a row of data fetched from datomic, will
  extrect a single field. It will first check if the requested field was fetched
  directly in the `:find` clause, if not it will resolve the entity and get the
  attribute from there."
  [{:keys [find]} entity-fn field]
  (if-let [idx (index-of find field)]
    (par nth idx)
    (if (list? field)
      (let [[attr ?eid] field
            idx (index-of find ?eid)]
        (assert (qualified-keyword? attr))
        (assert (symbol? ?eid))
        (if idx
          (comp attr entity-fn (par nth idx))
          (let [idx (index-of find (symbol (str ?eid "|"
                                                (namespace attr) "|"
                                                (name attr))))]
            (assert idx)
            (par nth idx)
            ))))))

(defn select-fields [qry entity-fn fields]
  (apply juxt (map (pal select-field qry entity-fn) fields)))

(defn order-clause->comparator [qry entity-fn order-by]
  (fn [x y]
    (reduce (fn [result [dir field]]
              (if (= 0 result)
                (*
                 (if (= :desc dir) -1 1)
                 (compare ((select-field qry entity-fn field) x)
                          ((select-field qry entity-fn field) y)))
                (reduced result)))
            0
            order-by)))

(defn order-by-attribs [qry entity-fn order-by results]
  (if (seq order-by)
    (sort (order-clause->comparator qry entity-fn order-by) results)
    results))

(defn resolve-fields [db result {:keys [select order-by] :as datalog}]
  (let [entity-fn (memoize (fn [eid] (d/entity db eid)))]
    (->> result
         (order-by-attribs datalog entity-fn order-by)
         (map (select-fields datalog entity-fn select)))))

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

(defn execute-query [{:keys [native query] :as native-query}]
  (let [db      (or (:db native) (db))
        datalog (read-string (:query native))
        results (d/q (dissoc datalog :fields) db)
        x       (if query
                  (result-map-mbql db results datalog query)
                  (result-map-native db results datalog))]
    (reset! reslt x)
    x))
