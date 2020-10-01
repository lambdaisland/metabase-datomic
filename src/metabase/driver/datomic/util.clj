(ns metabase.driver.datomic.util
  (:import java.util.Date)
  (:require [clojure.string :as str]))

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
  java.util.UUID
  (lt [x y]
      (and (instance? java.util.UUID y) (< (.compareTo x y) 0)))
  (gt [x y]
      (and (instance? java.util.UUID y) (> (.compareTo x y) 0)))
  (lte [x y]
       (and (instance? java.util.UUID y) (<= (.compareTo x y) 0)))
  (gte [x y]
       (and (instance? java.util.UUID y) (>= (.compareTo x y) 0)))

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
    (and (string? y) (>= (.compareTo x y) 0)))

  clojure.lang.Keyword
  (lt [x y]
    (and (keyword? y) (< (.compareTo (name x) (name y)) 0)))
  (gt [x y]
    (and (keyword? y) (> (.compareTo (name x) (name y)) 0)))
  (lte [x y]
    (and (keyword? y) (<= (.compareTo (name x) (name y)) 0)))
  (gte [x y]
    (and (keyword? y) (>= (.compareTo (name x) (name y)) 0))))

(defn str-starts-with? [s prefix {case? :case-sensitive}]
  (if case?
    (str/starts-with? (str s)
                      (str prefix))
    (str/starts-with? (str/lower-case (str s))
                      (str/lower-case (str prefix)))))

(defn str-ends-with? [s prefix {case? :case-sensitive}]
  (if case?
    (str/ends-with? (str s)
                    (str prefix))
    (str/ends-with? (str/lower-case (str s))
                    (str/lower-case (str prefix)))))

(defn str-contains? [s prefix {case? :case-sensitive}]
  (if case?
    (str/includes? (str s)
                   (str prefix))
    (str/includes? (str/lower-case (str s))
                   (str/lower-case (str prefix)))))
