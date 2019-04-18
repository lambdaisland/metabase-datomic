(ns metabase.driver.datomic.query-processor
  (:require [clojure.string :as str]
            [datomic.api :as d]
            [metabase.driver.datomic.util :as util :refer [pal par]]
            [metabase.mbql.util :as mbql.u]
            [metabase.models.field :as field :refer [Field]]
            [metabase.models.table :refer [Table]]
            [metabase.query-processor.store :as qp.store]
            [toucan.db :as db]))

;; Local variable naming conventions:

;; dqry  : Datalog query
;; mbqry : Metabase (MBQL) query
;; db    : Datomic DB instance

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
  ([dqry clause coll]
   (into-clause dqry clause identity coll))
  ([dqry clause xform coll]
   (if (seq coll)
     (update dqry clause (fn [x] (into (or x []) xform coll)))
     dqry)))

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
;; rename to table-sym ?
(defmulti ->eid mbql.u/dispatch-by-clause-name-or-class)

(defmethod ->eid (class Table) [{:keys [name]}]
  (symbol (str "?" name)))

(defmethod ->eid (class Field) [{:keys [table_id]}]
  (->eid (qp.store/table table_id)))

(defmethod ->eid Integer [table_id]
  (->eid (qp.store/table table_id)))

(defmethod ->eid :field-id [[_ field-id]]
  (->eid (qp.store/field field-id)))

(declare aggregation-clause)
(declare field-sym)

(def field-ref nil)
(defmulti field-ref
  "Turn an MBQL field reference into something we can stick into our
  pseudo-datalag :select clause. In some cases we stick the same thing in :find,
  in others we parse this form after the fact to pull the data out of the
  entity.

  Closely related to field-sym, but the latter is always just a single
  symbol (logic variable), but with sections separated by | which can be parsed.

  [:field 15] ;;=> (:artist/name ?artist)
  [:datetime-field [:field 25] :hour] ;;=> (datetime (:user/logged-in ?user) :hour)
  [:aggregation 0] ;;=> (count ?artist)"
  (fn [_ field-ref]
    (mbql.u/dispatch-by-clause-name-or-class field-ref)))

(defmethod field-ref :default [_ field-ref]
  (field-sym field-ref))

(defmethod field-ref :field-id [_ field-ref]
  (list (->attrib field-ref)
        (->eid field-ref)))

(defmethod field-ref :aggregation [mbqry [_ idx]]
  (aggregation-clause mbqry (nth (:aggregation mbqry) idx)))

(defmethod field-ref :datetime-field [mbqry [_ fref unit]]
  `(datetime ~(field-ref mbqry fref) ~unit))

(defn apply-source-table [dqry {:keys [source-table breakout] :as mbqry}]
  (if (seq breakout)
    dqry
    (let [table   (qp.store/table source-table)
          eid     (->eid table)
          fields  (db/select Field :table_id source-table)
          attribs (remove (comp reserved-prefixes namespace)
                          (map ->attrib fields))
          clause  `(~'or ~@(map #(vector eid %) attribs))]
      (-> dqry
          (into-clause :where [clause])))))

;; Entries in the :fields clause can be
;;
;; | Concrete field refrences | [:field-id 15]           |
;; | Expression references    | [:expression :sales_tax] |
;; | Aggregates               | [:aggregate 0]           |
;; | Foreign keys             | [:fk-> 10 20]            |
(defn apply-fields [dqry {:keys [source-table fields order-by] :as mbqry}]
  (if (seq fields)
    (-> dqry
        (into-clause :find [(->eid source-table)])
        (into-clause :select (map (partial field-ref mbqry)) fields))
    dqry))

;; "[:field-id 45] ;;=> ?artist|artist|name"
(defmulti field-sym mbql.u/dispatch-by-clause-name-or-class)

(defmethod field-sym :field-id [field-ref]
  (let [attr (->attrib field-ref)
        eid (->eid field-ref)]
    (symbol (str eid "|" (namespace attr) "|" (name attr)))))

(defmethod field-sym :datetime-field [[_ ref unit]]
  (symbol (str (field-sym ref) "|" (name unit))))

;;=> [:field-id 45] ;;=> [[?artist :artist/name ?artist|artist|name]]
(defmulti field-bindings mbql.u/dispatch-by-clause-name-or-class)

(defmethod field-bindings :field-id [field-ref]
  [[(->eid field-ref)
    (->attrib field-ref)
    (field-sym field-ref)]])

(defmethod field-bindings :datetime-field [[_ field-ref unit :as dt-field]]
  (conj (field-bindings field-ref)
        [`(metabase.util.date/date-trunc-or-extract ~unit ~(field-sym field-ref))
         (field-sym dt-field)]))

;; breakouts with aggregation = GROUP BY
;; breakouts without aggregation = SELECT DISTINCT
(defn apply-breakouts [dqry {:keys [breakout order-by aggregation] :as mbqry}]
  (if (seq breakout)
    (-> dqry
        (into-clause :find (map field-sym) breakout)
        (into-clause :where (mapcat field-bindings) breakout)
        (into-clause :select (map (partial field-ref mbqry)) breakout)
        #_(cond-> #_dqry
            (empty? aggregation)
            (into-clause :with (map ->eid) breakout)))
    dqry))

(defmulti aggregation-clause (fn [mbqry aggregation]
                               (first aggregation)))

(defmethod aggregation-clause :count [mbqry [_ field-ref]]
  (if field-ref
    (list 'count (field-sym field-ref))
    (list 'count (->eid (:source-table mbqry)))))

(defmethod aggregation-clause :sum [mbqry [_ field-ref]]
  (list 'sum (field-sym field-ref)))

(defmulti apply-aggregation (fn [mbqry dqry aggregation]
                              (first aggregation)))

(defmethod apply-aggregation :default [mbqry dqry aggregation]
  (let [clause (aggregation-clause mbqry aggregation)
        field-ref (second aggregation)]
    (-> dqry
        (into-clause :find [clause])
        (into-clause :select [clause])
        (cond-> #_dqry
          field-ref
          (into-clause :where (field-bindings field-ref))))))

(defn apply-aggregations [dqry {:keys [aggregation] :as mbqry}]
  (reduce (partial apply-aggregation mbqry) dqry aggregation))

(defn apply-order-by [dqry {:keys [order-by] :as mbqry}]
  (if (seq order-by)
    (-> dqry
        (into-clause :order-by
                     (map (juxt first (comp (partial field-ref mbqry) second)))
                     order-by))
    dqry))

(def apply-filter nil)
(defmulti apply-filter (fn [_ [filter-clause _]]
                         filter-clause))

(defmethod apply-filter := [dqry [_ field [_ value]]]
  (if (= (->attrib field) :db/id)
    (-> dqry
        (into-clause :where [[(list '= (->eid field) value)]]))
    (-> dqry
        (into-clause :where [[(->eid field) (->attrib field) value]]))))

(defn apply-filters [dqry {:keys [filter]}]
  (if (seq filter)
    (apply-filter dqry filter)
    dqry))

(defn mbql->native [{database :database
                     mbqry :query}]
  {:query
   (with-out-str
     (clojure.pprint/pprint
      (-> {}
          (apply-source-table mbqry)
          (apply-fields mbqry)
          (apply-filters mbqry)
          (apply-order-by mbqry)
          (apply-breakouts mbqry)
          (apply-aggregations mbqry))))})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; EXECUTE-QUERY

(defn index-of [xs x]
  (loop [idx 0
         [y & xs] xs]
    (if (= x y)
      idx
      (if (seq xs)
        (recur (inc idx) xs)))))

;; Field selection
;;
;; This is the process where we take the :select clause, and compare it with
;; the :find clause, to see how to convert each row of data into the stuff
;; that's being asked for.
;;
;; Example 1:
;; {:find [?eid] :select [(:artist/name ?eid)]}
;;
;; Not too hard, look up the (d/entity ?eid) and get :artist/name attribute
;;
;; Example 2:
;; {:find [?artist|artist|name] :select [(:artist/name ?artist)]}
;;
;; The attribute is already fetched directly, so just use it.

(defmulti select-field-form (fn [dqry entity-fn form]
                              (first form)))

(defmethod select-field-form :default [{:keys [find]} entity-fn [attr ?eid]]
  (assert (qualified-keyword? attr))
  (assert (symbol? ?eid))
  (if-let [idx (index-of find ?eid)]
    (fn [row]
      (-> row (nth idx) entity-fn attr))
    (let [attr-sym (symbol (str ?eid "|" (namespace attr) "|" (name attr)))
          idx      (index-of find attr-sym)]
      (assert idx)
      (fn [row]
        (nth row idx)))))

(defmethod select-field-form `datetime [dqry entity-fn [_ field unit]]
  (let [[attr ?eid] field
        field-sym (symbol (str/join "|" [?eid
                                         (namespace attr)
                                         (name attr)
                                         (name unit)]))]
    (if-let [idx (index-of (:find dqry field-sym) field-sym)]
      (fn [row]
        (nth row idx))
      (let [select-nested-field (select-field-form dqry entity-fn field)]
        (fn [row]
          (metabase.util.date/date-trunc-or-extract
           unit
           (select-nested-field row)))))))

(defn select-field
  "Returns a function which, given a row of data fetched from datomic, will
  extrect a single field. It will first check if the requested field was fetched
  directly in the `:find` clause, if not it will resolve the entity and get the
  attribute from there."
  [{:keys [find] :as dqry} entity-fn field]
  (if-let [idx (index-of find field)]
    (par nth idx)
    (if (list? field)
      (select-field-form dqry entity-fn field))))

(defn select-fields [dqry entity-fn fields]
  (apply juxt (map (pal select-field dqry entity-fn) fields)))

(defn order-clause->comparator [dqry entity-fn order-by]
  (fn [x y]
    (reduce (fn [result [dir field]]
              (if (= 0 result)
                (*
                 (if (= :desc dir) -1 1)
                 (compare ((select-field dqry entity-fn field) x)
                          ((select-field dqry entity-fn field) y)))
                (reduced result)))
            0
            order-by)))

(defn order-by-attribs [dqry entity-fn order-by results]
  (if (seq order-by)
    (sort (order-clause->comparator dqry entity-fn order-by) results)
    results))

(defn resolve-fields [db result {:keys [select order-by] :as dqry}]
  (let [entity-fn (memoize (fn [eid] (d/entity db eid)))]
    (->> result
         (order-by-attribs dqry entity-fn order-by)
         (map (select-fields dqry entity-fn select)))))

(defmulti col-name mbql.u/dispatch-by-clause-name-or-class)

(defmethod col-name :field-id [[_ id]]
  (:name (qp.store/field id)))

(defmethod col-name :datetime-field [[_ ref unit]]
  (str (col-name ref) ":" (name unit)))

(defn result-map-mbql
  "Result map for a query originating from Metabase directly. We have access to
  the original MBQL query."
  [db results dqry mbqry]
  (let [{:keys [source-table fields limit breakout aggregation]} mbqry]
    {:columns (concat (map col-name fields)
                      (map col-name breakout)
                      (if aggregation
                        [(name (ffirst aggregation))]))
     :rows    (resolve-fields db results dqry)}))

(defn result-map-native
  "Result map for a 'native' query entered directly by the user."
  [db results dqry]
  {:columns (map str (:find dqry))
   :rows (seq results)})

(def reslt (atom nil))

(defn execute-query [{:keys [native query] :as native-query}]
  (let [db      (db)
        dqry    (read-string (:query native))
        results (d/q (dissoc dqry :fields) db)
        x       (if query
                  (result-map-mbql db results dqry query)
                  (result-map-native db results dqry))]
    (reset! reslt x)
    x))

#_
(read-string (:query (:native (user.repl/nqry))))
#_
@reslt
