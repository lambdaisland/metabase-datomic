(ns metabase.driver.datomic
  (:require [metabase.driver :as driver]
            [metabase.driver.datomic.query-processor :as datomic.qp]
            [metabase.query-processor :as qp]
            [metabase.query-processor.store :as qp.store]
            [datomic.api :as d]
            [clojure.string :as str]))

(require 'metabase.driver.datomic.monkey-patch)

(driver/register! :datomic)

(defmethod driver/supports? [:datomic :basic-aggregations] [_ _] false)

(defmethod driver/can-connect? :datomic [_ {db :db}]
  (try
    (d/connect db)
    true
    (catch Exception e
      false)))

(defn attribute-names
  "Query db for all attribute names."
  [db]
  (d/q
   '{:find [[?i ...]]
     :where [[?e :db/ident ?i]
             [?e :db/valueType]]}
   db))

(defn derive-table-names
  "Find all \"tables\" i.e. all namespace prefixes used in attribute names."
  [db]
  (into #{}
        (comp (map namespace)
              (remove #{"fressian" "db" "db.alter" "db.excise" "db.install" "db.sys"}))
        (attribute-names db)))

#_
(defn db []
  (d/db (d/connect "datomic:free://localhost:4334/mbrainz")))

(defn table-columns
  "Given the name of a \"table\" (attribute namespace prefix), find all attribute
  names that occur in entities that have an attribute with this prefix."
  [db table]
  (let [attrs (filter #(= (namespace %) table) (attribute-names db))]
    (d/q
     {:find '[?c ?tt]
      :where [(cons 'or
                    (for [attr attrs]
                      ['?e attr]))
              '[?e ?b]
              '[?b :db/ident ?c]
              '[?b :db/valueType ?t]
              '[?t :db/ident ?tt]
              '[(not= ?c :db/ident)]]}
     db)))

#_
(table-columns (db) "artist")

(defmethod driver/describe-database :datomic [_ instance]
  (let [url (get-in instance [:details :db])
        table-names (derive-table-names (d/db (d/connect url)))]
    {:tables
     (set
      (for [table-name table-names]
        {:name   table-name
         :schema nil}))}))

(def datomic->metabase-type
  {:db.type/keyword :type/Name       ;; Value type for keywords.
   :db.type/string  :type/Text       ;; Value type for strings.
   :db.type/boolean :type/Boolean    ;; Boolean value type.
   :db.type/long    :type/Integer    ;; Fixed integer value type. Same semantics as a Java long: 64 bits wide, two's complement binary representation.
   :db.type/bigint  :type/BigInteger ;; Value type for arbitrary precision integers. Maps to java.math.BigInteger on Java platforms.
   :db.type/float   :type/Float      ;; Floating point value type. Same semantics as a Java float: single-precision 32-bit IEEE 754 floating point.
   :db.type/double  :type/Float      ;; Floating point value type. Same semantics as a Java double: double-precision 64-bit IEEE 754 floating point.
   :db.type/bigdec  :type/Decimal    ;; Value type for arbitrary precision floating point numbers. Maps to java.math.BigDecimal on Java platforms.
   :db.type/ref     :type/Integer    ;; Value type for references. All references from one entity to another are through attributes with this value type.
   :db.type/instant :type/Time       ;; Value type for instants in time. Stored internally as a number of milliseconds since midnight, January 1, 1970 UTC. Maps to java.util.Date on Java platforms.
   :db.type/uuid    :type/UUID       ;; Value type for UUIDs. Maps to java.util.UUID on Java platforms.
   :db.type/uri     :type/URL        ;; Value type for URIs. Maps to java.net.URI on Java platforms.
   :db.type/bytes   :type/Array} ;; Value type for small binary data. Maps to byte array on Java platforms. See limitations.
  )

(defn kw->str [s]
  (str (namespace s) "/" (name s)))

(defmethod driver/describe-table :datomic [_ instance {table-name :name}]
  (let [url  (get-in instance [:details :db])
        db   (d/db (d/connect url))
        cols (table-columns db table-name)]
    {:name   table-name
     :schema nil
     :fields (into #{{:name          "db/id"
                      :database-type "db.type/ref"
                      :base-type     :type/Integer
                      :pk?           true}}
                   (for [[col type] cols]
                     {:name          (kw->str col)
                      :database-type (kw->str type)
                      :display-name  (str/capitalize (kw->str col))
                      :base-type     (datomic->metabase-type type)}))}))

#_
(driver/describe-table :datomic {:details {:db "datomic:free://localhost:4334/mbrainz"}}
                       {:name "artist"})


(defmethod driver/mbql->native :datomic [_ {:keys [database query] :as mbql-query}]
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
     :query {:find  [(list 'pull '?e fields)]
             :where [`(~'or ~@(map #(vector '?e %) columns))]}}))

(defmethod driver/execute-query :datomic [_ {:keys [native query] :as native-query}]
  (let [{:keys [source-table fields limit]} query

        db      (:db native)
        table   (qp.store/table source-table)
        columns (map first (table-columns db (:name table)))
        fields  (mapv (comp keyword
                            :name
                            (fn [[_ f]] (qp.store/field f)))
                      fields)
        results (d/q (:query native) db)]

    {:columns columns
     :rows    (map #(select-keys (first %) fields) results)} ))


(comment
  (d/q (:query (qp/query->native +q+)) (db))

  (table-columns (db) "country")

  (qp/process-query +q+)

  (def +q+
    {:database 3,
     :type :query,
     :query
     {:source-table 10,
      :fields
      [[:field-id 80] [:field-id 81] [:field-id 82] [:field-id 83] [:field-id 84]],
      :limit 10000},
     :middleware {:format-rows? false, :skip-results-metadata? true},
     :driver :datomic,
     :settings {}})



  (qp/query->native
    {:database 2,
     :type :query,
     :query
     {:source-table 10,
      :fields
      [[:field-id 80] [:field-id 81] [:field-id 82] [:field-id 83] [:field-id 84]],
      :limit 10000},
     :middleware {:format-rows? false, :skip-results-metadata? true},
     :driver :datomic,
     :settings {}})

  (qp.store/database)
  (qp.store/table 10)
  #metabase.models.database.DatabaseInstance{:id 2, :engine :datomic, :name "mbrau", :details {:db "datomic:free://localhost:4334/mbrainz", :ssl true}, :features #{:case-sensitivity-string-filter-options}}

  (require 'metabase.query-processor.middleware.resolve-fields)

  (alter-var-root #'qp.store/*store* (constantly (atom {})))

  ((metabase.query-processor.middleware.resolve-database/resolve-database identity) {:database 2})
  ((metabase.query-processor.middleware.resolve-fields/resolve-fields identity) {:source-table 10,
                                                                                 :fields
                                                                                 [[:field-id 80] [:field-id 81] [:field-id 82] [:field-id 83] [:field-id 84]],
                                                                                 :limit 10000})

  @@#'qp.store/*store*



  )
