(ns metabase.test.data.datomic
  (:require [metabase.test.data.interface :as tx]
            [metabase.test.data.sql :as sql.tx]
            [metabase.driver.datomic :as datomic-driver]
            [metabase.driver :as driver]
            [datomic.api :as d]))

(driver/add-parent! :datomic ::tx/test-extensions)

(def metabase->datomic-type
  (into {:type/DateTime :db.type/instant
         :type/Date     :db.type/instant}
        (map (fn [[k v]] [v k]))
        datomic-driver/datomic->metabase-type))

(defmethod sql.tx/field-base-type->sql-type [:datomic :type/*] [_ t]
  (metabase->datomic-type t))

(defn- db-url [dbdef]
  (str "datomic:mem:" (tx/escaped-name dbdef)))

(defn field->attributes
  [table-name {:keys [field-name base-type fk field-comment]}]
  (cond-> {:db/ident (keyword table-name field-name)
           :db/valueType (if fk
                           :db.type/ref
                           (metabase->datomic-type base-type))
           :db/cardinality :db.cardinality/one}
    field-comment
    (assoc :db/doc field-comment)))

(defn table->attributes
  [{:keys [table-name field-definitions rows table-comment]}]
  (map (partial field->attributes table-name) field-definitions))

(defn table->entities
  [{:keys [table-name field-definitions rows]}]
  (let [attrs (map (comp (partial keyword table-name)
                         :field-name)
                   field-definitions)]
    (->> rows
         (map (partial map (fn [field value]
                             (if-let [dest-table (:fk field)]
                               ;; Turn foreign keys ints into temp-id strings
                               (str (name dest-table) "-" value)
                               (if (= :type/BigInteger (:base-type field))
                                 (java.math.BigInteger/valueOf value)
                                 value)))
                       field-definitions))
         (map (partial zipmap attrs))
         (map (fn [attr-map]
                (into {} (filter (comp some? val)) attr-map)))
         (map (fn [idx attr-map]
                ;; Add temp-id strings to resolve foreign keys
                (assoc attr-map :db/id (str table-name "-" idx)))
              (next (range))))))

(defmethod tx/create-db! :datomic
  [_ {:keys [table-definitions] :as dbdef} & {:keys [skip-drop-db?] :as opts}]
  (let [url (db-url dbdef)]
    (when-not skip-drop-db?
      (d/delete-database url))
    (d/create-database url)
    (let [conn   (d/connect url)
          schema (mapcat table->attributes table-definitions)
          data   (mapcat table->entities table-definitions)]
      @(d/transact conn schema)
      @(d/transact conn data))))

(defmethod tx/dbdef->connection-details :datomic [_ context dbdef]
  {:db (db-url dbdef)})

(comment
  (letsc 9 (mapcat table->attributes (:table-definitions dbdef)))
  (def dbdef (letsc 7 dbdef))
  (table->attributes (first (:table-definitions dbdef)))
  )
