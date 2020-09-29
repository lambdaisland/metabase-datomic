(ns user.setup
  (:require [toucan.db :as db]
            [metabase.setup :as setup]
            [metabase.models.user :as user :refer [User]]
            [metabase.models.database :as database :refer [Database]]
            [metabase.models.field :as field :refer [Field]]
            [metabase.models.table :as table :refer [Table]]
            [metabase.public-settings :as public-settings]
            [metabase.sync :as sync]
            [clojure.java.io :as io]
            [datomic.api :as d]))

(defn setup-first-user []
  (let [new-user (db/insert! User
                   :email        "admin@example.com"
                   :first_name   "dev"
                   :last_name    "dev"
                   :password     (str (java.util.UUID/randomUUID))
                   :is_superuser true)]
    (user/set-password! (:id new-user) "dev")))

(defn setup-site []
  (public-settings/site-name "Acme Inc.")
  (public-settings/admin-email "arne@example.com")
  (public-settings/anon-tracking-enabled false)
  (setup/clear-token!))

(defn setup-database
  ([]
   (setup-database "MusicBrainz" "datomic:free://localhost:4334/mbrainz" {}))
  ([name url config]
   (let [dbinst (db/insert! Database
                  {:name name
                   :engine :datomic
                   :details {:db url
                             :config (pr-str config)}
                   :is_on_demand false
                   :is_full_sync true
                   :cache_field_values_schedule "0 50 0 * * ? *"
                   :metadata_sync_schedule "0 50 * * * ? *"})]
     (sync/sync-database! dbinst))))

(defn remove-database
  ([]
   (remove-database  "MusicBrainz"))
  ([name]
   (remove-database name false))
  ([name remove-datomic-db?]
   (let [dbinst (db/select-one Database :name name)
         tables (db/select Table :db_id (:id dbinst))
         fields (db/select Field {:where [:in :table_id (map :id tables)]})]
     (when (= :datomic (:engine dbinst))
       (d/delete-database (-> dbinst :details :db)))
     (db/delete! Field {:where [:in :id (map :id fields)]})
     (db/delete! Table {:where [:in :id (map :id tables)]})
     (db/delete! Database :id (:id dbinst)))))

(defn reset-database!
  ([]
   (remove-database)
   (setup-database))
  ([name]
   (remove-database name)
   (setup-database name)))

(defn setup-all []
  (setup-first-user)
  (setup-site)
  (setup-database))
