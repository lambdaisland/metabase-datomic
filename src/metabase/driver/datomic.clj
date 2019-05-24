(ns metabase.driver.datomic
  (:require [datomic.api :as d]
            [metabase.driver :as driver]
            [metabase.driver.datomic.query-processor :as datomic.qp]
            [metabase.driver.datomic.util :as util]
            [toucan.db :as db]
            [clojure.tools.logging :as log]))

(require 'metabase.driver.datomic.monkey-patch)

(driver/register! :datomic)

(def features
  {:basic-aggregations                     true
   :standard-deviation-aggregations        true
   :case-sensitivity-string-filter-options true
   :foreign-keys                           true
   :nested-queries                         true
   :expressions                            false
   :expression-aggregations                false
   :native-parameters                      false
   :binning                                false})

(doseq [[feature] features]
  (defmethod driver/supports? [:datomic feature] [_ _]
    (get features feature)))

(defmethod driver/can-connect? :datomic [_ {db :db}]
  (try
    (d/connect db)
    true
    (catch Exception e
      false)))

(defmethod driver/describe-database :datomic [_ instance]
  (let [url (get-in instance [:details :db])
        table-names (datomic.qp/derive-table-names (d/db (d/connect url)))]
    {:tables
     (set
      (for [table-name table-names]
        {:name   table-name
         :schema nil}))}))

(defn user-config [database]
  (try
    (read-string (get-in database [:details :config] "{}"))
    (catch Exception e
      (log/error e "Datomic EDN is not configured correctly."))))

(derive :type/Keyword :type/Text)

(def datomic->metabase-type
  {:db.type/keyword :type/Keyword    ;; Value type for keywords.
   :db.type/string  :type/Text       ;; Value type for strings.
   :db.type/boolean :type/Boolean    ;; Boolean value type.
   :db.type/long    :type/Integer    ;; Fixed integer value type. Same semantics as a Java long: 64 bits wide, two's complement binary representation.
   :db.type/bigint  :type/BigInteger ;; Value type for arbitrary precision integers. Maps to java.math.BigInteger on Java platforms.
   :db.type/float   :type/Float      ;; Floating point value type. Same semantics as a Java float: single-precision 32-bit IEEE 754 floating point.
   :db.type/double  :type/Float      ;; Floating point value type. Same semantics as a Java double: double-precision 64-bit IEEE 754 floating point.
   :db.type/bigdec  :type/Decimal    ;; Value type for arbitrary precision floating point numbers. Maps to java.math.BigDecimal on Java platforms.
   :db.type/ref     :type/FK         ;; Value type for references. All references from one entity to another are through attributes with this value type.
   :db.type/instant :type/DateTime   ;; Value type for instants in time. Stored internally as a number of milliseconds since midnight, January 1, 1970 UTC. Maps to java.util.Date on Java platforms.
   :db.type/uuid    :type/UUID       ;; Value type for UUIDs. Maps to java.util.UUID on Java platforms.
   :db.type/uri     :type/URL        ;; Value type for URIs. Maps to java.net.URI on Java platforms.
   :db.type/bytes   :type/Array      ;; Value type for small binary data. Maps to byte array on Java platforms. See limitations.
   })

(defn column-name [table-name col]
  (if (= (namespace col)
         table-name)
    (name col)
    (util/kw->str col)))

(defn describe-table [database {table-name :name}]
  (let [url    (get-in database [:details :db])
        config (user-config database)
        db     (d/db (d/connect url))
        cols   (datomic.qp/table-columns db table-name)
        rels   (get-in config [:relationships (keyword table-name)])]
    {:name   table-name
     :schema nil

     ;; Fields *must* be a set
     :fields
     (-> #{{:name          "db/id"
            :database-type "db.type/ref"
            :base-type     :type/PK
            :pk?           true}}
         (into (for [[col type] cols]
                 {:name          (column-name table-name col)
                  :database-type (util/kw->str type)
                  :base-type     (datomic->metabase-type type)
                  :special-type  (datomic->metabase-type type)}))
         (into (for [[rel-name {:keys [path target]}] rels]
                 {:name          (name rel-name)
                  :database-type "metabase.driver.datomic/path"
                  :base-type     :type/FK
                  :special-type  :type/FK})))}))

(defmethod driver/describe-table :datomic [_ database table]
  (describe-table database table))

(defn guess-dest-column [db table-names col]
  (let [table? (into #{} table-names)
        attrs (d/q (assoc '{:find [[?ident ...]]}
                     :where [['_ col '?eid]
                             '[?eid ?attr]
                             '[?attr :db/ident ?ident]])
                   db)]
    (or (some->> attrs
                 (map namespace)
                 (remove #{"db"})
                 frequencies
                 (sort-by val)
                 last
                 key)
        (table? (name col)))))

(defn describe-table-fks [database {table-name :name}]
  (let [url    (get-in database [:details :db])
        db     (d/db (d/connect url))
        config (user-config database)
        tables (datomic.qp/derive-table-names db)
        cols   (datomic.qp/table-columns db table-name)
        rels   (get-in config [:relationships (keyword table-name)])]

    (-> #{}
        (into (for [[col type] cols
                    :when      (= type :db.type/ref)
                    :let       [dest (guess-dest-column db tables col)]
                    :when      dest]
                {:fk-column-name   (column-name table-name col)
                 :dest-table       {:name   dest
                                    :schema nil}
                 :dest-column-name "db/id"}))
        (into (for [[rel-name {:keys [path target]}] rels]
                {:fk-column-name (name rel-name)
                 :dest-table {:name (name target)
                              :schema nil}
                 :dest-column-name "db/id"})))))

(defmethod driver/describe-table-fks :datomic [_ database table]
  (describe-table-fks database table))

(defonce mbql-history (atom ()))
(defonce query-history (atom ()))

(defmethod driver/mbql->native :datomic [_ query]
  (swap! mbql-history conj query)
  (datomic.qp/mbql->native query))

(defmethod driver/execute-query :datomic [_ native-query]
  (swap! query-history conj native-query)
  (let [result (datomic.qp/execute-query native-query)]
    (swap! query-history conj result)
    result))
