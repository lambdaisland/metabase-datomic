(ns metabase.driver.datomic-test
  (:require [expectations :refer [expect]]
            [metabase.test.data.datasets :refer [expect-with-driver]]
            [metabase.test.util :as tu]))

#_
(expect-with-driver :datomic
                    "UTC"
                    (tu/db-timezone-id))
