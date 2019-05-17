(ns metabase.driver.datomic.util
  (:import java.util.Date))

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

;; Polymorphic and type forgiving comparisons, for use in generated queries.
(defprotocol Comparisons
  (lt [x y])
  (gt [x y])
  (lte [x y])
  (gte [x y]))

(extend-protocol Comparisons
  java.lang.Number
  (lt [x y]
    (and (number? y) (< x y)))
  (gt [x y]
    (and (number? y) (> x y)))
  (lte [x y]
    (and (number? y) (<= x y)))
  (gte [x y]
    (and (number? y) (>= x y)))

  java.util.Date
  (lt [x y]
    (and (inst? y) (< (inst-ms x) (inst-ms y))))
  (gt [x y]
    (and (inst? y) (> (inst-ms x) (inst-ms y))))
  (lte [x y]
    (and (inst? y) (<= (inst-ms x) (inst-ms y))))
  (gte [x y]
    (and (inst? y) (>= (inst-ms x) (inst-ms y))))

  clojure.core.Inst
  (lt [x y]
    (and (inst? y) (< (inst-ms x) (inst-ms y))))
  (gt [x y]
    (and (inst? y) (> (inst-ms x) (inst-ms y))))
  (lte [x y]
    (and (inst? y) (<= (inst-ms x) (inst-ms y))))
  (gte [x y]
    (and (inst? y) (>= (inst-ms x) (inst-ms y))))

  java.lang.String
  (lt [x y]
    (and (string? y) (< (.compareTo x y) 0)))
  (gt [x y]
    (and (string? y) (> (.compareTo x y) 0)))
  (lte [x y]
    (and (string? y) (<= (.compareTo x y) 0)))
  (gte [x y]
    (and (string? y) (<= (.compareTo x y) 0)))

  clojure.lang.Keyword
  (lt [x y]
    (and (keyword? y) (< (.compareTo (name x) (name y)) 0)))
  (gt [x y]
    (and (keyword? y) (> (.compareTo (name x) (name y)) 0)))
  (lte [x y]
    (and (keyword? y) (<= (.compareTo (name x) (name y)) 0)))
  (gte [x y]
    (and (keyword? y) (<= (.compareTo (name x) (name y)) 0))))
