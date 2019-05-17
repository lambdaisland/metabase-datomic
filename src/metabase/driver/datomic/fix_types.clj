(ns metabase.driver.datomic.fix-types
  (:require [metabase.models.database :refer [Database]]
            [metabase.models.field :refer [Field]]
            [metabase.models.table :refer [Table]]
            [toucan.db :as db]))

(defn undo-invalid-primary-keys!
  "Only :db/id should ever be marked as PK, unfortunately Metabase heuristics can
  also mark other fields as primary, which leads to a mess. This changes the
  \"special_type\" of any field with special_type=PK back to nil, unless the
  field is called :db/id."
  []
  (db/update-where! Field
                    {:table_id [:in {:select [:id]
                                     :from [:metabase_table]
                                     :where [:in :db_id {:select [:id]
                                                         :from [:metabase_database]
                                                         :where [:= :engine "datomic"]}]}]
                     :database_type [:not= "db.type/ref"]
                     :special_type "type/PK"}
                    :special_type nil))
