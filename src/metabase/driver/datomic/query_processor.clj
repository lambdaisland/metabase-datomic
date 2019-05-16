(ns metabase.driver.datomic.query-processor
  (:require [clojure.string :as str]
            [datomic.api :as d]
            [metabase.driver.datomic.util :as util :refer [pal par]]
            [metabase.mbql.util :as mbql.u]
            [metabase.models.field :as field :refer [Field]]
            [metabase.models.table :refer [Table]]
            [metabase.query-processor.store :as qp.store]
            [toucan.db :as db]
            [clojure.set :as set]))

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

(def ^:dynamic *settings* {})

(defn- timezone-id
  []
  (or (:report-timezone *settings*) "UTC"))

(declare aggregation-clause)
(declare field-sym)

(defn into-clause
  "Helper to build up datalog queries. Takes a partial query, a clause like :find
  or :where, and a collection, and appends the collection's elements into the
  given clause.

  Optionally takes a transducer."
  ([dqry clause coll]
   (into-clause dqry clause identity coll))
  ([dqry clause xform coll]
   (if (seq coll)
     (update dqry clause (fn [x] (into (or x []) xform coll)))
     dqry)))

(defn distinct-preseed
  "Like clojure.core distinct, but preseed the 'seen' collection."
  [coll]
  (fn [rf]
    (let [seen (volatile! (set coll))]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result input]
         (if (contains? @seen input)
           result
           (do (vswap! seen conj input)
               (rf result input))))))))

(defn into-clause-uniq
  "Like into-clause, but assures that the same clause is never inserted twice."
  ([dqry clause coll]
   (into-clause-uniq dqry clause identity coll))
  ([dqry clause xform coll]
   (into-clause dqry
                clause
                (comp xform
                      (distinct-preseed (get dqry clause)))
                coll)))

;; [:field-id 55] => :artist/name
(defmulti ->attrib mbql.u/dispatch-by-clause-name-or-class)

(defmethod ->attrib (class Field) [{:keys [name table_id] :as field}]
  (if (some #{\/} name)
    (keyword name)
    (keyword (:name (qp.store/table table_id)) name)))

(defmethod ->attrib :field-id [[_ field-id]]
  (->attrib (qp.store/field field-id)))

(defmethod ->attrib :fk-> [[_ src dst]]
  (->attrib dst))

(defmethod ->attrib :aggregation [[_ field-id]]
  (->attrib (qp.store/field field-id)))

;; [:field-id 55] => ?artist
;; rename to table-sym ?
(defmulti table-sym mbql.u/dispatch-by-clause-name-or-class)

(defmethod table-sym (class Table) [{:keys [name]}]
  (symbol (str "?" name)))

(defmethod table-sym (class Field) [{:keys [table_id]}]
  (table-sym (qp.store/table table_id)))

(defmethod table-sym Integer [table_id]
  (table-sym (qp.store/table table_id)))

(defmethod table-sym :field-id [[_ field-id]]
  (table-sym (qp.store/field field-id)))

(defmethod table-sym :fk-> [[_ src dst]]
  (symbol (str (field-sym src) "->" (subs (str (table-sym dst)) 1))))

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
        (table-sym field-ref)))

(defmethod field-ref :aggregation [mbqry [_ idx]]
  (aggregation-clause mbqry (nth (:aggregation mbqry) idx)))

(defmethod field-ref :datetime-field [mbqry [_ fref unit]]
  `(datetime ~(field-ref mbqry fref) ~unit))

(defmethod field-ref :fk-> [mbqry [_ src dest :as field]]
  (list (->attrib field) (table-sym field)))

(defn apply-source-table [dqry {:keys [source-table breakout] :as mbqry}]
  (if (seq breakout)
    dqry
    (let [table   (qp.store/table source-table)
          eid     (table-sym table)
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
(defn apply-fields [dqry {:keys [source-table join-tables fields order-by] :as mbqry}]
  (if (seq fields)
    (-> dqry
        (into-clause :find [(table-sym source-table)])
        (into-clause :select (map (partial field-ref mbqry)) fields)
        (cond-> #_dqry
          (seq join-tables)
          (-> (into-clause :find
                           (map (fn [{:keys [table-id fk-field-id pk-field-id]}]
                                  (table-sym
                                   [:fk->
                                    [:field-id fk-field-id]
                                    [:field-id pk-field-id]])))
                           join-tables)
              (into-clause-uniq :where
                                (map (fn [{:keys [table-id fk-field-id pk-field-id]}]
                                       [(table-sym source-table)
                                        (->attrib [:field-id fk-field-id])
                                        (table-sym
                                         [:fk->
                                          [:field-id fk-field-id]
                                          [:field-id pk-field-id]])]))
                                join-tables))))
    dqry))

;; "[:field-id 45] ;;=> ?artist|artist|name"
(defmulti field-sym mbql.u/dispatch-by-clause-name-or-class)

(defmethod field-sym :field-id [field-ref]
  (let [attr (->attrib field-ref)
        eid (table-sym field-ref)]
    (if (= :db/id attr)
      eid
      (symbol (str eid "|" (namespace attr) "|" (name attr))))))

(defmethod field-sym :datetime-field [[_ ref unit]]
  (symbol (str (field-sym ref) "|" (name unit))))

(defmethod field-sym :fk-> [[_ src dst]]
  (symbol (str (field-sym src) "->" (subs (str (field-sym dst)) 1))))

(defn ident-sym [field-ref]
  (symbol (str (field-sym field-ref) ":ident")))

(defmulti constant-binding (fn [field-ref value]
                             (mbql.u/dispatch-by-clause-name-or-class field-ref)))

(defmethod constant-binding :field-id [field-ref value]
  (let [attr (->attrib field-ref)
        ?eid (table-sym field-ref)]
    (if (= :db/id attr)
      (if (keyword? value)
        (let [ident-sym (ident-sym field-ref)]
          [[?eid :db/ident value]])
        [[(list '= ?eid value)]])
      [[?eid attr value]])))

(defmethod constant-binding :fk-> [[_ src dst] value]
  (let [src-attr (->attrib src)
        dst-attr (->attrib dst)
        ?src (table-sym src)
        ?dst (table-sym dst)]
    (if (= :db/id dst-attr)
      (if (keyword? value)
        (let [?ident (ident-sym field-ref)]
          [[?src src-attr ?ident]
           [?ident :db/ident value]])
        [[?src src-attr value]])
      [[?src src-attr ?dst]
       [?dst dst-attr value]])))

;;=> [:field-id 45] ;;=> [[?artist :artist/name ?artist|artist|name]]
(defmulti field-bindings mbql.u/dispatch-by-clause-name-or-class)

(defmethod field-bindings :field-id [field-ref]
  (let [attr (->attrib field-ref)]
    (when-not (= :db/id attr)
      [[(table-sym field-ref) attr (field-sym field-ref)]])))

(defmethod field-bindings :datetime-field [[_ field-ref unit :as dt-field]]
  (conj (field-bindings field-ref)
        [`(metabase.util.date/date-trunc-or-extract ~unit ~(field-sym field-ref) ~(timezone-id))
         (field-sym dt-field)]))

(defmethod field-bindings :fk-> [[_ src dst :as field]]
  (if (= :db/id (->attrib field))
    [[(table-sym src) (->attrib src) (table-sym field)]]
    [[(table-sym src) (->attrib src) (table-sym field)]
     [(table-sym field) (->attrib field) (field-sym field)]]))

;; breakouts with aggregation = GROUP BY
;; breakouts without aggregation = SELECT DISTINCT
(defn apply-breakouts [dqry {:keys [breakout order-by aggregation] :as mbqry}]
  (if (seq breakout)
    (-> dqry
        (into-clause-uniq :find (map field-sym) breakout)
        (into-clause-uniq :where (mapcat field-bindings) breakout)
        (into-clause :select (map (partial field-ref mbqry)) breakout)
        #_(cond-> #_dqry
            (empty? aggregation)
            (into-clause :with (map table-sym) breakout)))
    dqry))

(defmulti aggregation-clause (fn [mbqry aggregation]
                               (first aggregation)))

(defmethod aggregation-clause :default [mbqry [aggr-type field-ref]]
  (list (symbol (name aggr-type)) (field-sym field-ref)))

(defmethod aggregation-clause :count [mbqry [_ field-ref]]
  (if field-ref
    (list 'count (field-sym field-ref))
    (list 'count (table-sym (:source-table mbqry)))))

(defmethod aggregation-clause :distinct [mbqry [_ field-ref]]
  (list 'count-distinct (field-sym field-ref)))

(defmulti apply-aggregation (fn [mbqry dqry aggregation]
                              (first aggregation)))

(defmethod apply-aggregation :default [mbqry dqry aggregation]
  (let [clause                (aggregation-clause mbqry aggregation)
        [aggr-type field-ref] aggregation]
    (-> dqry
        (into-clause-uniq :find [clause])
        (into-clause :select [clause])
        (cond-> #_dqry
          (#{:avg :sum :stddev} aggr-type)
          (into-clause-uniq :with [(table-sym field-ref)])
          field-ref
          (into-clause-uniq :where (field-bindings field-ref))))))

(defn apply-aggregations [dqry {:keys [aggregation] :as mbqry}]
  (reduce (partial apply-aggregation mbqry) dqry aggregation))

(defn apply-order-by [dqry {:keys [order-by] :as mbqry}]
  (if (seq order-by)
    (-> dqry
        (into-clause :order-by
                     (map (juxt first (comp (partial field-ref mbqry) second)))
                     order-by))
    dqry))

(defmulti value-literal
  "Extracts the value literal out of form like and coerce to the DB type.

     [:value 40
      {:base_type :type/Float
       :special_type :type/Latitude
       :database_type \"db.type/double\"}]"
  (fn [[type :as clause]] type))

(defmethod value-literal :default [clause]
  (assert false (str "Unrecognized value clause: " clause)))

(defmethod value-literal :value [[t v f]]
  (case (:database_type f)
    "db.type/ref"
    (if (string? v)
      (if (some #{\/} v)
        (keyword v)
        (Long/parseLong v))
      v)

    "db.type/string"
    (str v)

    "db.type/long"
    (if (string? v)
      (Long/parseLong v)
      v)

    "db.type/float"
    (if (string? v)
      (Float/parseFloat v)
      v)

    "db.type/uri"
    (if (string? v)
      (java.net.URI. v)
      v)

    v))

(defmethod value-literal :absolute-datetime [[_ inst unit]]
  (metabase.util.date/date-trunc-or-extract unit inst (timezone-id)))

(defmethod value-literal :relative-datetime [[_ offset unit]]
  (if (= :current offset)
    (java.util.Date.)
    (metabase.util.date/relative-date unit offset)))

(defmulti filter-clauses (fn [[clause-type _]]
                           clause-type))


(defmethod filter-clauses := [[_ field-ref [_ _ {base-type :base_type
                                                 :as       field-inst}
                                            :as vclause]]]
  (constant-binding field-ref (value-literal vclause)))

(defmethod filter-clauses :and [[_ & clauses]]
  (into [] (mapcat filter-clauses) clauses))

(defn logic-vars
  "Recursively find all logic var symbols starting with a question mark."
  [clause]
  (cond
    (coll? clause)
    (into #{} (mapcat logic-vars) clause)

    (and (symbol? clause)
         (= \? (first (name clause))))
    [clause]

    :else
    []))

(defmethod filter-clauses :or [[_ & clauses]]
  (let [clauses (map filter-clauses clauses)
        lvars   (apply set/intersection (map logic-vars clauses))]
    (assert (pos-int? (count lvars))
      (str "No logic variables found to unify across [:or] in " (pr-str `[:or ~@clauses])))

    ;; Only bind any logic vars shared by all clauses in the outer clause. This
    ;; will prevent Datomic from complaining, but could potentially be too
    ;; naive. Since typically all clauses filter the same entity though this
    ;; should generally be good enough.
    [`(~'or-join [~@lvars]
       ~@(map (fn [c]
                (if (= (count c) 1)
                  (first c)
                  (cons 'and c)))
              clauses))]))

(defmethod filter-clauses :< [[_ field value]]
  (conj (field-bindings field)
        [`(util/lt ~(field-sym field) ~(value-literal value))]))

(defmethod filter-clauses :> [[_ field value]]
  (conj (field-bindings field)
        [`(util/gt ~(field-sym field) ~(value-literal value))]))

(defmethod filter-clauses :<= [[_ field value]]
  (conj (field-bindings field)
        [`(util/lte ~(field-sym field) ~(value-literal value))]))

(defmethod filter-clauses :>= [[_ field value]]
  (conj (field-bindings field)
        [`(util/gte ~(field-sym field) ~(value-literal value))]))

(defmethod filter-clauses :!= [[_ field value]]
  (conj (field-bindings field)
        [`(not= ~(field-sym field) ~(value-literal value))]))

(defmethod filter-clauses :between [[_ field min-val max-val]]
  (into (field-bindings field)
        [[`(util/lte ~(value-literal min-val) ~(field-sym field))]
         [`(util/lte ~(field-sym field) ~(value-literal max-val))]]))

(defn apply-filters [dqry {:keys [filter]}]
  (if (seq filter)
    (into-clause-uniq dqry :where (filter-clauses filter))
    dqry))

(defn clean-up-with-clause
  "If a logic variable appears in both an aggregate in the :find clause, and in
  the :with clause, then this will seriously confuse the Datomic query engine.
  In this scenario having the logic variable in `:with' is superfluous, having
  it in an aggregate achieves the same thing.

  This scenario occurs when e.g. having both a [:count] and [:sum $field]
  aggregate. The count doesn't have a field, so it counts the entities, the sum
  adds a with clause for the entity id to prevent merging of duplicates,
  resulting in Datomic trying to unify the variable with itself. "
  [{:keys [find] :as dqry}]
  (update dqry
          :with
          (pal remove
               (set (concat
                     find
                     (keep (fn [clause]
                             (and (list? clause)
                                  (second clause)))
                           find))))))

(defn mbql->native [{database :database
                     mbqry :query
                     settings :settings}]
  (binding [*settings* settings]
    {:query
     (with-out-str
       (clojure.pprint/pprint
        (-> {}
            (apply-source-table mbqry)
            (apply-fields mbqry)
            (apply-filters mbqry)
            (apply-order-by mbqry)
            (apply-breakouts mbqry)
            (apply-aggregations mbqry)
            (clean-up-with-clause))))}))

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

(defn ref? [entity-fn attr]
  (let [attr-entity (entity-fn attr)]
    (when (= :db.type/ref (:db/valueType attr-entity))
      attr-entity)))

(defn entity-map?
  "Is the object an EntityMap, i.e. the result of calling datomic.api/entity."
  [x]
  (instance? datomic.query.EntityMap x))

(defn unwrap-entity [val]
  (if (entity-map? val)
    (:db/id val)
    val))

(defmethod select-field-form :default [{:keys [find]} entity-fn [attr ?eid]]
  (assert (qualified-keyword? attr))
  (assert (symbol? ?eid))
  (if-let [idx (index-of find ?eid)]
    (fn [row]
      (let [entity (-> row (nth idx) entity-fn)]
        (if (= :db/id attr)
          #_(:db/ident entity (:db/id entity))
          (:db/id entity)
          (unwrap-entity (attr entity)))))
    (let [attr-sym (symbol (str ?eid "|" (namespace attr) "|" (name attr)))
          idx      (index-of find attr-sym)]
      (assert idx)
      (fn [row]
        (let [value (nth row idx)]
          ;; Try to convert enum-style ident references back to keywords
          (if-let [attr-entity (and (integer? value) (ref? entity-fn attr))]
            (or (d/ident (d/entity-db attr-entity) value)
                value)
            value))))))

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
           (select-nested-field row)
           (timezone-id)))))))

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

(defn cartesian-product
  "Expand any set results (references with cardinality/many) to their cartesian
  products. Empty sets produce a single row with a nil value (similar to an
  outer join).

  (cartesian-product [1 2 #{:a :b}])
  ;;=> ([1 2 :b] [1 2 :a])

  (cartesian-product [1 #{:x :y} #{:a :b}])
  ;;=> ([1 :y :b] [1 :y :a] [1 :x :b] [1 :x :a])

  (cartesian-product [1 2 #{}])
  ;;=> ([1 2 nil])"
  [row]
  (reduce (fn [res value]
            (if (set? value)
              (if (seq value)
                (for [r res
                      v value]
                  (conj r v))
                (map (par conj nil) res))
              (map (par conj value) res)))
          [[]]
          row))

(defn entity->db-id [val]
  (if (instance? datomic.Entity val)
    (if-let [ident (:db/ident val)]
      (str ident)
      (:db/id val))
    val))

(defn resolve-fields [db result {:keys [select order-by] :as dqry}]
  (let [entity-fn (memoize (fn [eid] (d/entity db eid)))]
    (->> result
         ;; TODO: This needs to be retought, we can only really order after
         ;; expanding set references (cartesian-product). Currently breaks when
         ;; sorting on cardinality/many fields.
         (order-by-attribs dqry entity-fn order-by)
         (map (select-fields dqry entity-fn select))
         (mapcat cartesian-product)
         (map (pal map entity->db-id)))))

(defmulti col-name mbql.u/dispatch-by-clause-name-or-class)

(defmethod col-name :field-id [[_ id]]
  (:name (qp.store/field id)))

(defmethod col-name :datetime-field [[_ ref unit]]
  (str (col-name ref) ":" (name unit)))

(defmethod col-name :fk-> [[_ src dest]]
  (col-name dest))

(def aggr-col-name nil)
(defmulti aggr-col-name first)

(defmethod aggr-col-name :default [[aggr-type]]
  (name aggr-type))

(defmethod aggr-col-name :distinct [_]
  "count")

(defn result-map-mbql
  "Result map for a query originating from Metabase directly. We have access to
  the original MBQL query."
  [db results dqry mbqry]
  (let [{:keys [source-table fields limit breakout aggregation]} mbqry]
    {:columns (concat (map col-name fields)
                      (map col-name breakout)
                      (map aggr-col-name aggregation))
     :rows    (resolve-fields db results dqry)}))

(defn result-map-native
  "Result map for a 'native' query entered directly by the user."
  [db results dqry]
  {:columns (map str (:find dqry))
   :rows (seq results)})

(defn execute-query [{:keys [native query] :as native-query}]
  (let [db      (db)
        dqry    (read-string (:query native))
        results (d/q (dissoc dqry :fields) db)
        ;; Hacking around this is as it's so common in Metabase's automatic
        ;; dashboards. Datomic never returns a count of zero, instead it just
        ;; returns an empty result.
        results (if (and (empty? results) (= (:aggregation query) [[:count]]))
                  [[0]]
                  results)]
    (if query
      (result-map-mbql db results dqry query)
      (result-map-native db results dqry))))
