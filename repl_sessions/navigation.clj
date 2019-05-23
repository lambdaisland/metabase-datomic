(ns navigation
  (:require [datomic.api :as d]))

{:relationships {:journal-entry
                 {:account
                  {:path [:journal-entry/journal-entry-lines :journal-entry-line/account]
                   :target :account}}}}



{:rules
 [[(sub-accounts ?p ?c)
   [?p :account/children ?c]]
  [(sub-accounts ?p ?d)
   [?p :account/children ?c]
   (sub-accounts ?c ?d)]
  ]

 :relationships
 {:account
  {:journal-entry-lines
   {:path [:journal-entry-line/_account]
    :target :journal-entry-line}

   :subaccounts
   {:path [sub-accounts]
    :target :account}

   :parent-accounts
   {:path [_sub-accounts]
    :target :account}
   }

  :journal-entry-line
  {:fiscal-year
   {:path [:journal-entry/_journal-entry-lines
           :ledger/_journal-entries
           :fiscal-year/_ledgers]
    :target :fiscal-year}}

  }}

(defn path-binding [from-sym to-sym path]
  (let [segment-binding
        (fn [from to seg]
          (cond
            (keyword? seg)
            (if (= \_ (first (name seg)))
              [to (keyword (namespace seg) (subs (name seg) 1)) from]
              [from seg to])

            (symbol? seg)
            (if (= \_ (first (name seg)))
              (list (symbol (subs (str seg) 1)) to from)
              (list seg from to))))]
    (loop [[p & path] path
           binding    []
           from-sym   from-sym]
      (if (seq path)
        (let [next-from (gensym "?")]
          (recur path
                 (conj binding (segment-binding from-sym next-from p))
                 next-from))
        (conj binding (segment-binding from-sym to-sym p))))))

(path-binding '?jel '?fj '[:journal-entry/_journal-entry-lines
                           :ledger/_journal-entries
                           :fiscal-year/_ledgers])


(d/q '{:find [?e]
       :where [[?e :db/ident]]} (db))

(user/refer-repl)
