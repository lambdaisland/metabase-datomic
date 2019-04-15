(ns metabase.driver.datomic.util)

(defn kw->str [s]
  (str (namespace s) "/" (name s)))

(def pal
  "Partial-left (same as clojure.core/partial)"
  partial)

(defn par
  "Partial-right, partially apply rightmost function arguments."
  [f & xs]
  (fn [& ys]
    (apply f (concat ys xs))))
