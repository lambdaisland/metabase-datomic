(ns metabase.driver.datomic.util)

(defn kw->str [s]
  (str (namespace s) "/" (name s)))
