(ns user.setup
  (:require [toucan.db :as db]
            [metabase.setup :as setup]
            [metabase.models.user :as user :refer [User]]
            [metabase.models.database :as database :refer [Database]]
            [metabase.public-settings :as public-settings]
            [clojure.java.io :as io]
            ))

(defn setup-first-user []
  (let [new-user (db/insert! User
                   :email        "arne@example.com"
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

(defn setup-database []
  (db/insert! Database
    {:name "MusicBrainz"
     :engine :datomic
     :details {:db "datomic:free://localhost:4334/mbrainz"}
     :is_on_demand false
     :is_full_sync true
     :cache_field_values_schedule "0 50 0 * * ? *"
     :metadata_sync_schedule "0 50 * * * ? *"}))

(defn setup-all []
  (setup-first-user)
  (setup-site)
  (setup-driver)
  (setup-database))
