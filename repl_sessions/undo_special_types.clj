(ns undo-special-types
  (:require [metabase.models.database :refer [Database]]
            [metabase.models.field :refer [Field]]
            [metabase.models.table :refer [Table]]
            [toucan.db :as db]))

(db/update-where! Field
    {:table_id [:in {:select [:id]
                     :from [:metabase_table]
                     :where [:in :db_id {:select [:id]
                                         :from [:metabase_database]
                                         :where [:= :engine "datomic"]}]}]
     :database_type [:not= "db.type/ref"]
     :special_type "type/PK"}
  :special_type nil)
