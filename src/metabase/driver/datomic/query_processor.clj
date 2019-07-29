(ns metabase.driver.datomic.query-processor
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [datomic.api :as d]
            [metabase.driver.datomic.util :as util :refer [pal par]]
            [metabase.mbql.util :as mbql.u]
            [metabase.models.field :as field :refer [Field]]
            [metabase.models.table :refer [Table]]
            [metabase.query-processor.store :as qp.store]
            [toucan.db :as db]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk])
  (:import java.net.URI
           java.util.UUID))

;; Local variable naming conventions:

;; dqry  : Datalog query
;; mbqry : Metabase (MBQL) query
;; db    : Datomic DB instance
;; attr  : A datomic attribute, i.e. a qualified keyword
;; ?foo / lvar : A logic variable, i.e. a symbol starting with a question mark

(def connect #_(memoize d/connect)
  d/connect)

(defn user-config
  ([]
   (user-config (qp.store/database)))
  ([database]
   (try
     (let [edn (get-in database [:details :config])]
       (read-string (or edn "{}")))
     (catch Exception e
       (log/error e "Datomic EDN is not configured correctly.")
       {}))))

(defn tx-filter []
  (when-let [form (get (user-config) :tx-filter)]
    (eval form)))

(defn db []
  (let [db (-> (get-in (qp.store/database) [:details :db]) connect d/db)]
    (if-let [pred (tx-filter)]
      (d/filter db pred)
      db)))

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

(defn attrs-by-table
  "Map from table name to collection of attribute entities."
  [db]
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; QUERY->NATIVE

(def ^:dynamic *settings* {})

(def ^:dynamic *mbqry* nil)

(defn- timezone-id
  []
  (or (:report-timezone *settings*) "UTC"))

(defn source-table []
  (:source-table *mbqry* (:source-table (:source-query *mbqry*))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:dynamic
  *db*
  "Datomic db, for when we need to inspect the schema during query generation."
  nil)

(defn cardinality-many?
  "Is the given keyword an reference attribute with cardinality/many?"
  [attr]
  (= :db.cardinality/many (:db/cardinality (d/entity *db* attr))))

(defn attr-type [attr]
  (get-in
   (d/pull *db* [{:db/valueType [:db/ident]}] attr)
   [:db/valueType :db/ident]))

(defn entid [ident]
  (d/entid *db* ident))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Datalog query helpers
;;
;; These functions all handle various parts of building up complex Datalog
;; queries based on the given MBQL query.

(declare aggregation-clause)
(declare field-lvar)

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; [:field-id 55] => :artist/name
(defmulti ->attrib
  "Convert an MBQL field reference (vector) to a Datomic attribute name (qualified
  symbol)."
  mbql.u/dispatch-by-clause-name-or-class)

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti field-inst
  "Given an MBQL field reference, return the metabase.model.Field instance. May
  return nil."
  mbql.u/dispatch-by-clause-name-or-class)

(defmethod field-inst :field-id [[_ id]]
  (qp.store/field id))

(defmethod field-inst :fk-> [[_ src _]]
  (field-inst src))

(defmethod field-inst :datetime-field [[_ field _]]
  (field-inst field))

(defmethod field-inst :field-literal [[_ literal]]
  (when-let [table (source-table)]
    (db/select-one Field :table_id table :name literal)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; [:field-id 55] => ?artist
(defmulti table-lvar
  "Return a logic variable name (a symbol starting with '?') corresponding with
  the 'table' of the given field reference."
  mbql.u/dispatch-by-clause-name-or-class)

(defmethod table-lvar (class Table) [{:keys [name]}]
  (symbol (str "?" name)))

(defmethod table-lvar (class Field) [{:keys [table_id]}]
  (table-lvar (qp.store/table table_id)))

(defmethod table-lvar Integer [table_id]
  (table-lvar (qp.store/table table_id)))

(defmethod table-lvar :field-id [[_ field-id]]
  (table-lvar (qp.store/field field-id)))

(defmethod table-lvar :fk-> [[_ src dst]]
  (field-lvar src))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti field-lookup
  "Turn an MBQL field reference into something we can stick into our
  pseudo-datalag :select or :order-by clauses. In some cases we stick the same
  thing in :find, in others we parse this form after the fact to pull the data
  out of the entity.

  Closely related to field-lvar, but the latter is always just a single
  symbol (logic variable), but with sections separated by | which can be parsed.

  [:field 15] ;;=> (:artist/name ?artist)
  [:datetime-field [:field 25] :hour] ;;=> (datetime (:user/logged-in ?user) :hour)
  [:aggregation 0] ;;=> (count ?artist)"
  (fn [_ field-ref]
    (mbql.u/dispatch-by-clause-name-or-class field-ref)))

(defmethod field-lookup :default [_ field-ref]
  `(field ~(field-lvar field-ref)
          ~(select-keys (field-inst field-ref) [:database_type :base_type :special_type])))

(defmethod field-lookup :aggregation [mbqry [_ idx]]
  (aggregation-clause mbqry (nth (:aggregation mbqry) idx)))

(defmethod field-lookup :datetime-field [mbqry [_ fref unit]]
  `(datetime ~(field-lvar fref) ~unit))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Note that lvar in this context always means *logic variable* (in the
;; prolog/datalog sense), not to be confused with lval/rval in the C/C++ sense.

(defn lvar
  "Generate a logic variable, a symbol starting with a question mark, by combining
  multiple pieces separated by pipe symbols. Parts are converted to string and
  leading question marks stripped, so you can combined lvars into one bigger
  lvar, e.g. `(lvar '?foo '?bar)` => `?foo|bar`"
  [& parts]
  (symbol
   (str "?" (str/join "|" (map (fn [p]
                                 (let [s (str p)]
                                   (cond-> s
                                     (= \: (first s))
                                     (subs 1)
                                     (= \? (first s))
                                     (subs 1))))
                               parts)))))

;; "[:field-id 45] ;;=> ?artist|artist|name"
(defmulti field-lvar
  "Convert an MBQL field reference like [:field-id 45] into a logic variable named
  based on naming conventions (see Architecture Decision Log).

  Will look like this:

  ?table|attr-namespace|attr-name
  ?table|attr-namespace|attr-name|time-binning-unit
  ?table|attr-namespace|attr-name->fk-dest-table|fk-attr-ns|fk-attr-name"
  mbql.u/dispatch-by-clause-name-or-class)

(defmethod field-lvar :field-id [field-ref]
  (let [attr (->attrib field-ref)
        eid (table-lvar field-ref)]
    (if (= :db/id attr)
      eid
      (lvar eid (namespace attr) (name attr)))))

(defmethod field-lvar :datetime-field [[_ ref unit]]
  (lvar (field-lvar ref) (name unit)))

(defmethod field-lvar :fk-> [[_ src dst]]
  (if (= "db/id" (:name (field-inst dst)))
    (field-lvar src)
    (lvar (str (field-lvar src) "->" (subs (str (field-lvar dst)) 1)))))

(defmethod field-lvar :field-literal [[_ field-name]]
  (if (some #{\|} field-name)
    (lvar field-name)
    (if-let [table (source-table)]
      (let [?table (table-lvar table)]
        (if (some #{\/} field-name)
          (lvar ?table field-name)
          (lvar ?table ?table field-name)))
      (lvar field-name))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ident-lvar [field-ref]
  (symbol (str (field-lvar field-ref) ":ident")))

;; Datomic function helpers
(defmacro %get-else% [& args] `(list '~'get-else ~@args))
(defmacro %count% [& args] `(list '~'count ~@args))
(defmacro %count-distinct% [& args] `(list '~'count-distinct ~@args))

(def NIL ::nil)
(def NIL-REF Long/MIN_VALUE)

;; Try to provide a type-appropriate placeholder, so that Datomic is able to
;; sort/group correctly
(def NIL_VALUES
  {:db.type/string  (str ::nil)
   :db.type/keyword ::nil
   :db.type/boolean false
   :db.type/bigdec  Long/MIN_VALUE
   :db.type/bigint  Long/MIN_VALUE
   :db.type/double  Long/MIN_VALUE
   :db.type/float   Long/MIN_VALUE
   :db.type/long    Long/MIN_VALUE
   :db.type/ref     Long/MIN_VALUE
   :db.type/instant #inst "0001-01-01T01:01:01"
   :db.type/uri     (URI. (str "nil" ::nil))
   :db.type/uuid    (UUID/randomUUID)})

(def ^:dynamic *strict-bindings* false)

(defmacro with-strict-bindings [& body]
  `(binding [*strict-bindings* true]
     ~@body))

(defn- bind-attr-reverse
  "When dealing with reverse attributes (:foo/_bar) either missing? or get-else
  don't work, and we need a different approach to get correct join semantics in
  the presence of missing data.

  Note that the caller takes care of removing the underscore from the attribute
  name and swapping the ?e and ?v."
  [?e a ?v]
  (if *strict-bindings*
    [?e a ?v]
    (list 'or-join [?e ?v]
          [?e a ?v]
          (list 'and (list 'not ['_ a ?v])
                [(list 'ground NIL-REF) ?e]))))

(defn reverse-attr? [attr]
  (when (= \_ (first (name attr)))
    (keyword (namespace attr) (subs (name attr) 1))))

(defn bind-attr
  "Datalog EAV binding that unifies to NIL if the attribute is not present, the
  equivalent of an outer join, so we can look up attributes without filtering
  the result at the same time.

  Will do a simple [e a v] binding when *strict-bindings* is true."
  [?e a ?v]
  (if-let [a (reverse-attr? a)]
    (bind-attr-reverse ?v a ?e)
    (cond
      *strict-bindings*
      [?e a ?v]

      ;; get-else is not supported on cardinality/many
      (cardinality-many? a)
      (list 'or-join [?e ?v]
            [?e a ?v]
            (list 'and
                  [(list 'missing? '$ ?e a)]
                  [(list 'ground NIL-REF) ?v]))

      :else
      [(%get-else% '$ ?e a (NIL_VALUES (attr-type a) ::nil)) ?v])))

(defn date-trunc-or-extract-some [unit date]
  (if (= NIL date)
    NIL
    (metabase.util.date/date-trunc-or-extract unit date (timezone-id))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn path-bindings
  "Given a start and end point (two lvars) and a path, a sequence of Datomic
  attributes (kw) and rule names (sym), generates a sequence of :where binding
  forms to navigate from start to symbol via the given path."
  [?from ?to path]
  (let [segment-binding
        (fn [?from ?to seg]
          (cond
            (keyword? seg)
            (bind-attr ?from seg ?to)

            (symbol? seg)
            (if (= \_ (first (name seg)))
              (list (symbol (subs (str seg) 1)) ?to ?from)
              (list seg ?from ?to))))]
    (loop [[p & path] path
           binding    []
           ?from      ?from]
      (if (seq path)
        (let [?next-from (lvar ?from (namespace p) (name p))]
          (recur path
                 (conj binding (segment-binding ?from ?next-from p))
                 ?next-from))
        (conj binding (segment-binding ?from ?to p))))))

(defn custom-relationship?
  "Is tis field reference a custom relationship, i.e. configured via the admin UI
  and backed by a custom path traversal."
  [field-ref]
  (-> field-ref
      field-inst
      :database_type
      (= "metabase.driver.datomic/path")))

(defn computed-field?
  "Is tis field reference a custom relationship, i.e. configured via the admin UI
  and backed by a custom path traversal."
  [field-ref]
  (-> field-ref
      field-inst
      :database_type
      (= "metabase.driver.datomic/computed-field")))

;;=> [:field-id 45] ;;=> [[?artist :artist/name ?artist|artist|name]]
(defmulti field-bindings
  "Given a field reference, return the necessary Datalog bindings (as used
  in :where) to bind the entity-eid to [[table-lvar]], and the associated value
  to [[field-lvar]].

  This uses Datomic's `get-else` to prevent filtering, in other words this will
  bind logic variables, but does not restrict the result."
  mbql.u/dispatch-by-clause-name-or-class)

(defmethod field-bindings :field-id [field-ref]
  (cond
    (custom-relationship? field-ref)
    (let [src-field             (field-inst field-ref)
          src-name              (keyword (:name (qp.store/table (:table_id src-field))))
          rel-name              (keyword (:name src-field))
          {:keys [path target]} (get-in (user-config) [:relationships src-name rel-name])]
      (path-bindings (table-lvar field-ref)
                     (field-lvar field-ref)
                     path))

    (computed-field? field-ref)
    (let [field          (field-inst field-ref)
          table-name     (keyword (:name (qp.store/table (:table_id field))))
          field-name     (keyword (:name field))
          {:keys [rule]} (get-in (user-config) [:fields table-name field-name])]
      [(list rule (table-lvar field-ref) (field-lvar field-ref))])

    :else
    (let [attr (->attrib field-ref)]
      (when-not (= :db/id attr)
        [(bind-attr (table-lvar field-ref) attr (field-lvar field-ref))]))))

(defmethod field-bindings :field-literal [[_ literal :as field-ref]]
  ;; This is dodgy, as field literals contain no source or schema information,
  ;; but this is used in native queries, so if we have a source table, and the
  ;; field name seems to correspond with an actual attribute with that prefix,
  ;; then we'll go for it. If not this retuns an empty seq i.e. doesn't bind
  ;; anything, and you will likely end up with Datomic complaining about
  ;; insufficient bindings.
  (if-let [table (source-table)]
    (let [attr (keyword (:name (qp.store/table table)) literal)]
      (if (attr-type attr)
        [(bind-attr (table-lvar table) attr (field-lvar field-ref))]
        []))
    []))

(defmethod field-bindings :fk-> [[_ src dst :as field]]
  (cond
    (custom-relationship? src)
    (let [src-field             (field-inst src)
          src-name              (keyword (:name (qp.store/table (:table_id src-field))))
          rel-name              (keyword (:name src-field))
          {:keys [path target]} (get-in (user-config) [:relationships src-name rel-name])
          attrib                (->attrib dst)
          path                  (if (= :db/id attrib)
                                  path
                                  (conj path attrib))]
      (path-bindings (table-lvar src)
                     (field-lvar field)
                     path))

    (computed-field? dst)
    (let [dst-field      (field-inst dst)
          table-name     (keyword (:name (qp.store/table (:table_id dst-field))))
          field-name     (keyword (:name dst-field))
          {:keys [rule]} (get-in (user-config) [:fields table-name field-name])]
      [(bind-attr (table-lvar src) (->attrib src) (field-lvar src))
       (list rule (field-lvar src) (field-lvar field))])

    (= :db/id (->attrib field))
    [[(table-lvar src) (->attrib src) (table-lvar field)]]

    :else
    [(bind-attr (table-lvar src) (->attrib src) (field-lvar src))
     (bind-attr (field-lvar src) (->attrib field) (field-lvar field))]))

(defmethod field-bindings :aggregation [_]
  [])

(defmethod field-bindings :datetime-field [[_ field-ref unit :as dt-field]]
  (conj (field-bindings field-ref)
        [`(date-trunc-or-extract-some ~unit ~(field-lvar field-ref))
         (field-lvar dt-field)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti constant-binding
  "Datalog bindings for filtering based on the value of an attribute (a constant
  value in MBQL), given a field reference and a value.

  At its simplest returns [[?table-lvar :attribute CONSTANT]], but can return
  more complex forms to deal with looking by :db/id, by :db/ident, or through
  foreign key."
  (fn [field-ref value]
    (mbql.u/dispatch-by-clause-name-or-class field-ref)))

(defmethod constant-binding :field-id [field-ref value]
  (let [attr (->attrib field-ref)
        ?eid (table-lvar field-ref)
        ?val (field-lvar field-ref)]
    (if (= :db/id attr)
      (if (keyword? value)
        [[?eid :db/ident value]]
        [[(list '= ?eid value)]])
      (conj (field-bindings field-ref)
            [(list 'ground value) ?val]))))

(defmethod constant-binding :field-literal [field-name value]
  [[(list 'ground value) (field-lvar field-name)]])

(defmethod constant-binding :datetime-field [[_ field-ref unit :as dt-field] value]
  (let [?val (field-lvar dt-field)]
    (conj (field-bindings dt-field)
          [(list '= (date-trunc-or-extract-some unit value) ?val)])))

(defmethod constant-binding :fk-> [[_ src dst :as field-ref] value]
  (let [src-attr (->attrib src)
        dst-attr (->attrib dst)
        ?src (table-lvar src)
        ?dst (field-lvar src)
        ?val (field-lvar field-ref)]
    (if (= :db/id dst-attr)
      (if (keyword? value)
        (let [?ident (ident-lvar field-ref)]
          [[?src src-attr ?ident]
           [?ident :db/ident value]])
        [[?src src-attr value]])
      (conj (field-bindings src)
            (bind-attr ?dst dst-attr ?val)
            [(list 'ground value) ?val]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti aggregation-clause
  "Return a Datalog clause for a given MBQL aggregation

  [:count [:field-id 45]]
  ;;=> (count ?foo|bar|baz)"
  (fn [mbqry aggregation]
    (first aggregation)))

(defmethod aggregation-clause :default [mbqry [aggr-type field-ref]]
  (list (symbol (name aggr-type)) (field-lvar field-ref)))

(defmethod aggregation-clause :count [mbqry [_ field-ref]]
  (cond
    field-ref
    (%count% (field-lvar field-ref))

    (source-table)
    (%count% (table-lvar (source-table)))

    :else
    (assert false "Count without field is not supported on native sub-queries.")))

(defmethod aggregation-clause :distinct [mbqry [_ field-ref]]
  (%count-distinct% (field-lvar field-ref)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti value-literal
  "Extracts the value literal out of and coerce to the DB type.

     [:value 40
      {:base_type :type/Float
       :special_type :type/Latitude
       :database_type \"db.type/double\"}]
    ;;=> 40"
  (fn [[type :as clause]] type))

(defmethod value-literal :default [clause]
  (assert false (str "Unrecognized value clause: " clause)))

(defmethod value-literal :value [[t v f]]
  (if (nil? v)
    (NIL_VALUES (keyword (:database_type f)) NIL)
    (case (:database_type f)
      "db.type/ref"
      (cond
        (and (string? v) (some #{\/} v))
        (entid (keyword v))

        (string? v)
        (Long/parseLong v)

        :else
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

      v)))

(defmethod value-literal :absolute-datetime [[_ inst unit]]
  (metabase.util.date/date-trunc-or-extract unit inst (timezone-id)))

(defmethod value-literal :relative-datetime [[_ offset unit]]
  (if (= :current offset)
    (java.util.Date.)
    (metabase.util.date/relative-date unit offset)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti filter-clauses
  "Convert an MBQL :filter form into Datalog :where clauses

  [:= [:field-id 45] [:value 20]]
  ;;=> [[?user :user/age 20]]"
  (fn [[clause-type _]]
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

(defn or-join
  "Takes a sequence of sequence of datalog :where clauses, each inner sequence is
  considered a single logical group, where clauses within one group are
  considered logical conjunctions (and), and generates an or-join, unifying
  those lvars that are present in all clauses.

  (or-join '[[[?venue :venue/location ?loc]
              [?loc :location/city \"New York\"]]
             [[?venue :venue/size 1000]]])
  ;;=>
  [(or-join [?venue]
      (and [?venue :venue/location ?loc]
           [?loc :location/city \"New York\"])
      [?venue :venue/size 1000])] "
  [clauses]
  (let [lvars   (apply set/intersection (map logic-vars clauses))]
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

(defmethod filter-clauses :or [[_ & clauses]]
  (or-join (map filter-clauses clauses)))

(defmethod filter-clauses :< [[_ field value]]
  (conj (field-bindings field)
        [`(util/lt ~(field-lvar field) ~(value-literal value))]))

(defmethod filter-clauses :> [[_ field value]]
  (conj (field-bindings field)
        [`(util/gt ~(field-lvar field) ~(value-literal value))]))

(defmethod filter-clauses :<= [[_ field value]]
  (conj (field-bindings field)
        [`(util/lte ~(field-lvar field) ~(value-literal value))]))

(defmethod filter-clauses :>= [[_ field value]]
  (conj (field-bindings field)
        [`(util/gte ~(field-lvar field) ~(value-literal value))]))

(defmethod filter-clauses :!= [[_ field value]]
  (conj (field-bindings field)
        [`(not= ~(field-lvar field) ~(value-literal value))]))

(defmethod filter-clauses :between [[_ field min-val max-val]]
  (into (field-bindings field)
        [[`(util/lte ~(value-literal min-val) ~(field-lvar field))]
         [`(util/lte ~(field-lvar field) ~(value-literal max-val))]]))

(defmethod filter-clauses :starts-with [[_ field value opts]]
  (conj (field-bindings field)
        [`(util/str-starts-with? ~(field-lvar field)
                                 ~(value-literal value)
                                 ~(merge {:case-sensitive true} opts))]))

(defmethod filter-clauses :ends-with [[_ field value opts]]
  (conj (field-bindings field)
        [`(util/str-ends-with? ~(field-lvar field)
                               ~(value-literal value)
                               ~(merge {:case-sensitive true} opts))]))

(defmethod filter-clauses :contains [[_ field value opts]]
  (conj (field-bindings field)
        [`(util/str-contains? ~(field-lvar field)
                              ~(value-literal value)
                              ~(merge {:case-sensitive true} opts))]))

(defmethod filter-clauses :not [[_ [_ field :as pred]]]
  (let [negate (fn [[e a :as pred]]
                 (if (= 'not e)
                   a
                   (list 'not pred)))
        pred-clauses (filter-clauses pred)
        {bindings true
         predicates false} (group-by (fn [[e a v :as clause]]
                                       (or (and (simple-symbol? e)
                                                (qualified-keyword? a)
                                                (simple-symbol? v))
                                           (and (list? e)
                                                (= 'get-else (first e)))))
                                     pred-clauses)]
    (if (= 1 (count predicates))
      (conj bindings (negate (first predicates)))
      (conj bindings (cons 'not predicates)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; MBQL top level constructs
;;
;; Each of these functions handles a single top-level MBQL construct
;; like :source-table, :fields, or :order-by, converting it incrementally into
;; the corresponding Datalog. Most of the heavy lifting is done by the Datalog
;; helper functions above.

(declare read-query)
(declare mbqry->dqry)

(defn apply-source-query
  "Nested query support. We don't actually 'nest' queries as Datalog doesn't have
  that, instead we merge queries, but keeping only the :find and :select parts
  of the outer query."
  [dqry {:keys [source-query fields breakout] :as mbqry}]
  (if source-query
    (cond-> (if-let [native (:native source-query)]
              (read-query native)
              (mbqry->dqry source-query))
      (or (seq fields) (seq breakout))
      (dissoc :find :select))
    dqry))

(defn source-table-clause [dqry {:keys [source-table breakout] :as mbqry}]
  (let [table   (qp.store/table source-table)
        eid     (table-lvar table)]
    (if-let [custom-clause (get-in (user-config) [:inclusion-clauses (keyword (:name table))])]
      (walk/postwalk-replace {'?eid eid} custom-clause)
      (let [fields  (db/select Field :table_id source-table)
            attribs (->> fields
                         (remove (comp #{"metabase.driver.datomic/path" "metabase.driver.datomic/computed-field"} :database_type))
                         (map ->attrib)
                         (remove (comp reserved-prefixes namespace)))]
        [`(~'or ~@(map #(vector eid %) attribs))]))))

(defn apply-source-table
  "Convert an MBQL :source-table clause into the corresponding Datalog. This
  generates a clause of the form

  (or [?user :user/first-name]
      [?user :user/last-name]
      [?user :user/name])

  In other words this binds the [[table-lvar]] to any entity that has any
  attributes corresponding with the given 'columns' of the given 'table'."
  [dqry {:keys [source-table] :as mbqry}]
  (if source-table
    (into-clause-uniq dqry :where (source-table-clause dqry mbqry))
    dqry))

;; Entries in the :fields clause can be
;;
;; | Concrete field refrences | [:field-id 15]           |
;; | Expression references    | [:expression :sales_tax] |
;; | Aggregates               | [:aggregate 0]           |
;; | Foreign keys             | [:fk-> 10 20]            |
(defn apply-fields [dqry {:keys [source-table join-tables fields order-by] :as mbqry}]
  (if (seq fields)
    (-> dqry
        (into-clause-uniq :find (map field-lvar) fields)
        (into-clause-uniq :where (mapcat field-bindings) fields)
        (into-clause :select (map (partial field-lookup mbqry)) fields))
    dqry))

;; breakouts with aggregation = GROUP BY
;; breakouts without aggregation = SELECT DISTINCT
(defn apply-breakouts [dqry {:keys [breakout order-by aggregation] :as mbqry}]
  (if (seq breakout)
    (-> dqry
        (into-clause-uniq :find (map field-lvar) breakout)
        (into-clause-uniq :where (mapcat field-bindings) breakout)
        (into-clause :select (map (partial field-lookup mbqry)) breakout))
    dqry))

(defn apply-aggregation [mbqry dqry aggregation]
  (let [clause                (aggregation-clause mbqry aggregation)
        [aggr-type field-ref] aggregation]
    (-> dqry
        (into-clause-uniq :find [clause])
        (into-clause :select [clause])
        (cond-> #_dqry
          (#{:avg :sum :stddev} aggr-type)
          (into-clause-uniq :with [(table-lvar field-ref)])
          field-ref
          (into-clause-uniq :where
                            (with-strict-bindings
                              (field-bindings field-ref)))))))

(defn apply-aggregations [dqry {:keys [aggregation] :as mbqry}]
  (reduce (partial apply-aggregation mbqry) dqry aggregation))

(defn apply-order-by [dqry {:keys [order-by aggregation] :as mbqry}]
  (if (seq order-by)
    (-> dqry
        (into-clause-uniq :find
                          (map (fn [[_ field-ref]]
                                 (if (= :aggregation (first field-ref))
                                   (aggregation-clause mbqry (nth aggregation (second field-ref)))
                                   (field-lvar field-ref))))
                          order-by)
        (into-clause-uniq :where (mapcat (comp field-bindings second)) order-by)
        (into-clause :order-by
                     (map (fn [[dir field-ref]]
                            [dir (field-lookup mbqry field-ref)]))
                     order-by))
    dqry))

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

(defn mbqry->dqry [mbqry]
  (-> '{:in [$ %]}
      (apply-source-query mbqry)
      (apply-source-table mbqry)
      (apply-fields mbqry)
      (apply-filters mbqry)
      (apply-order-by mbqry)
      (apply-breakouts mbqry)
      (apply-aggregations mbqry)
      (clean-up-with-clause)))

(defn mbql->native [{database :database
                     mbqry :query
                     settings :settings}]
  (binding [*settings* settings
            *db* (db)
            *mbqry* mbqry]
    {:query (-> mbqry
                mbqry->dqry
                clojure.pprint/pprint
                with-out-str)}))

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
        (unwrap-entity (get entity attr))))
    (let [attr-sym (lvar ?eid (namespace attr) (name attr))
          idx      (index-of find attr-sym)]
      (assert idx)
      (fn [row]
        (let [value (nth row idx)]
          ;; Try to convert enum-style ident references back to keywords
          (if-let [attr-entity (and (integer? value) (ref? entity-fn attr))]
            (or (d/ident (d/entity-db attr-entity) value)
                value)
            value))))))

(declare select-field)

(defmethod select-field-form `datetime [dqry entity-fn [_ field-lvar unit]]
  (if-let [row->field (select-field dqry entity-fn (lvar field-lvar unit))]
    row->field
    (let [row->field (select-field dqry entity-fn field-lvar)]
      (fn [row]
          (metabase.util.date/date-trunc-or-extract
           unit
           (row->field row)
           (timezone-id))))))

(defmethod select-field-form `field [dqry entity-fn [_ field {:keys [database_type]}]]
  (let [row->field (select-field dqry entity-fn field)]
    (fn [row]
      (let [value (row->field row)]
        (cond
          (and (= "db.type/ref" database_type) (integer? value))
          (:db/ident (entity-fn value) value)

          (and (= "metabase.driver.datomic/path" database_type) (integer? value))
          (:db/ident (entity-fn value) value)

          :else
          value)))))

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
                 (let [x ((select-field dqry entity-fn field) x)
                       y ((select-field dqry entity-fn field) y)]
                   (cond
                     (= x y) 0
                     (util/lt x y) -1
                     (util/gt x y) 1
                     :else (compare (str (class x))
                                    (str (class y))))))
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
  (let [nil-placeholder? (-> NIL_VALUES vals set
                             ;; false is used as a stand-in for nil in boolean
                             ;; fields, but we don't want to replace false with
                             ;; nil in results.
                             (disj false))
        entity-fn (memoize (fn [eid] (d/entity db eid)))]
    (->> result
         ;; TODO: This needs to be retought, we can only really order after
         ;; expanding set references (cartesian-product). Currently breaks when
         ;; sorting on cardinality/many fields.
         (order-by-attribs dqry entity-fn order-by)
         (map (select-fields dqry entity-fn select))
         (mapcat cartesian-product)
         (map (pal map entity->db-id))
         (map (pal map #(if (nil-placeholder? %)
                          nil
                          %))))))

(defmulti col-name mbql.u/dispatch-by-clause-name-or-class)

(defmethod col-name :field-id [[_ id]]
  (:name (qp.store/field id)))

(defmethod col-name :field-literal [[_ field]]
  field)

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

(defn lvar->col [lvar]
  (->> (str/split (subs (str lvar) 1) #"\|")
       reverse
       (take 2)
       reverse
       (str/join "_")))

(defn lookup->col [form]
  (cond
    (list? form)
    (str (first form))

    (symbol? form)
    (lvar->col form)

    :else
    (str form)))

(defn result-columns [dqry {:keys [source-query source-table fields limit breakout aggregation]}]
  (let [cols (concat (map col-name fields)
                     (map col-name breakout)
                     (map aggr-col-name aggregation))]
    (cond
      (seq cols)
      cols

      source-query
      (recur dqry source-query)

      (:select dqry)
      (map lookup->col (:select dqry))

      (:find dqry)
      (map lvar->col (:select dqry)))))

(defn result-map-mbql
  "Result map for a query originating from Metabase directly. We have access to
  the original MBQL query."
  [db results dqry mbqry]
  {:columns (result-columns dqry mbqry)
   :rows    (resolve-fields db results dqry)})

(defn result-map-native
  "Result map for a 'native' query entered directly by the user."
  [db results dqry]
  {:columns (map str (:find dqry))
   :rows (seq results)})

(defn read-query [q]
  #_(binding [*data-readers* (assoc *data-readers* 'metabase-datomic/nil (fn [_] NIL))])
  (let [qry (read-string q)]
    (if (vector? qry)
      (loop [key (first qry)
             val (take-while (complement keyword?) (next qry))
             qry (drop (inc (count val)) qry)
             res {key val}]
        (if (seq qry)
          (let [key (first qry)
                val (take-while (complement keyword?) (next qry))
                qry (drop (inc (count val)) qry)
                res (assoc res key val)]
            (recur key val qry res))
          res))
      qry)))


(defn execute-query [{:keys [native query] :as native-query}]
  (let [db      (db)
        dqry    (read-query (:query native))
        results (d/q (dissoc dqry :fields) db (:rules (user-config)))
        ;; Hacking around this is as it's so common in Metabase's automatic
        ;; dashboards. Datomic never returns a count of zero, instead it just
        ;; returns an empty result.
        results (if (and (empty? results)
                         (empty? (:breakout query))
                         (#{[[:count]] [[:sum]]} (:aggregation query)))
                  [[0]]
                  results)]
    (if query
      (result-map-mbql db results dqry query)
      (result-map-native db results dqry))))
